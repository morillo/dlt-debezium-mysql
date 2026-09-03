"""Delta Live Tables pipeline: Debezium (MySQL) CDC ingestion.

Bronze: raw Debezium JSON events ingested incrementally with Auto Loader.
Silver: flattened rows merged into a current-state table via apply_changes
(SCD type 1, deletes applied).

Pipeline configuration:
    pipeline.landing_root (required)  directory the Debezium events land
                                      in, e.g. a Unity Catalog volume
                                      "/Volumes/<catalog>/landing/debezium"
    pipeline.database     (required)  source MySQL database, e.g.
                                      "inventory"
    pipeline.tablename    (required)  source table name, e.g. "customers"
    pipeline.tablekeys    (optional)  comma-separated primary-key columns,
                                      defaults to "id"
    pipeline.topic        (optional)  Debezium topic / landing
                                      subdirectory; defaults to
                                      "mysql.<database>.<tablename>"

MySQL DECIMAL columns arrive from Debezium as base64-encoded bytes and are
kept as strings here; set decimal.handling.mode=string (or double) on the
Debezium connector if you need usable decimal values downstream.
"""

import json

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.getActiveSession()


def _required_setting(key: str) -> str:
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(
            f"Pipeline configuration '{key}' must be set (see the module "
            "docstring for all settings)."
        )
    return value


landing_root = _required_setting("pipeline.landing_root").rstrip("/")
database = _required_setting("pipeline.database")
table_name = _required_setting("pipeline.tablename")
key_columns = [
    k.strip()
    for k in spark.conf.get("pipeline.tablekeys", "id").split(",")
    if k.strip()
]
topic = spark.conf.get("pipeline.topic", None) or (
    f"mysql.{database}.{table_name}"
)

RAW_PATH = f"{landing_root}/{topic}"

# Kafka Connect physical types -> Spark types. JSON encodes bytes as base64
# strings, so "bytes" stays a string (see module docstring for decimals).
PHYSICAL_TYPES = {
    "int8": LongType(),
    "int16": LongType(),
    "int32": LongType(),
    "int64": LongType(),
    "float32": DoubleType(),
    "float64": DoubleType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "string": StringType(),
    "bytes": StringType(),
}

# Connect/Debezium logical types that can be converted after read. Values the
# schema encodes as epoch ints become proper dates/timestamps; time-of-day
# types (io.debezium.time.Time/MicroTime) have no Spark equivalent and pass
# through unchanged.
_EPOCH_DAYS_TO_DATE = "date_add(date'1970-01-01', cast({c} as int))"
LOGICAL_CONVERTERS = {
    "io.debezium.time.Date": _EPOCH_DAYS_TO_DATE,
    "org.apache.kafka.connect.data.Date": _EPOCH_DAYS_TO_DATE,
    "io.debezium.time.Timestamp": "timestamp_millis({c})",
    "org.apache.kafka.connect.data.Timestamp": "timestamp_millis({c})",
    "io.debezium.time.MicroTimestamp": "timestamp_micros({c})",
    "io.debezium.time.ZonedTimestamp": "cast({c} as timestamp)",
}


def load_latest_event_schema(path):
    """Return the Debezium envelope schema from the most recently written file.

    Reads only the raw ``schema`` field (no inference pass over the payloads)
    and picks the newest file so that after an upstream ALTER TABLE the
    current schema wins over stale variants. New columns still require a
    pipeline restart to be picked up.
    """
    rows = (
        spark.read.schema("schema STRING")
        .json(path)
        .select(
            "schema",
            F.col("_metadata.file_modification_time").alias("_mtime"),
        )
        .where(F.col("schema").isNotNull())
        .orderBy(F.col("_mtime").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        raise ValueError(
            "No Debezium event files found under {}. The pipeline cannot "
            "derive a schema until at least one event has landed.".format(path)
        )
    return json.loads(rows[0]["schema"])


def to_spark_type(field):
    ftype = field.get("type")
    if ftype not in PHYSICAL_TYPES:
        raise ValueError(
            "Unsupported Debezium field type '{}' for column '{}'.".format(
                ftype, field.get("field")
            )
        )
    return PHYSICAL_TYPES[ftype]


def build_payload_schema(envelope_schema):
    """Build the Spark schema for the event payload (before/after/source/...).

    The Debezium MySQL envelope nests at most one struct level, which is all
    this handles; a deeper struct raises rather than silently mistyping.
    """
    fields = []
    for f in envelope_schema["fields"]:
        nested = f.get("fields")
        if nested:
            nested_fields = []
            for nf in nested:
                if nf.get("fields"):
                    raise ValueError(
                        "Unexpected doubly-nested struct at '{}.{}'.".format(
                            f["field"], nf["field"]
                        )
                    )
                nested_fields.append(
                    StructField(nf["field"], to_spark_type(nf), True)
                )
            fields.append(
                StructField(f["field"], StructType(nested_fields), True)
            )
        else:
            fields.append(StructField(f["field"], to_spark_type(f), True))
    return StructType(fields)


def row_logical_types(envelope_schema):
    """Map sanitized row-column names to their Connect logical type names."""
    for f in envelope_schema["fields"]:
        if f["field"] == "after":
            return {
                nf["field"].replace(" ", "_"): nf["name"]
                for nf in f.get("fields") or []
                if nf.get("name")
            }
    return {}


event_schema = load_latest_event_schema(RAW_PATH)
payload_schema = build_payload_schema(event_schema)
logical_types = row_logical_types(event_schema)

# year/month/day/hour come from the directory partitioning of the raw files.
envelope = StructType(
    [
        StructField("payload", payload_schema, True),
        StructField("schema", StringType(), True),
        StructField("year", LongType(), True),
        StructField("month", LongType(), True),
        StructField("day", LongType(), True),
        StructField("hour", LongType(), True),
    ]
)


@dlt.table(
    name="bronze_{}".format(table_name),
    comment="Raw Debezium CDC events for {}.{}, incrementally "
    "ingested with Auto Loader".format(database, table_name),
    table_properties={"quality": "bronze"},
)
def bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(envelope)
        .load(RAW_PATH)
    )


def flatten_struct(schema, prefix=""):
    """Flatten a struct into aliased column references.

    The alias drops the outermost struct name, joins deeper levels with "_",
    and replaces spaces, so ``row_selected.`a b`.c`` becomes ``a_b_c``.
    """
    cols = []
    for elem in schema:
        path = prefix + elem.name
        if isinstance(elem.dataType, StructType):
            cols += flatten_struct(elem.dataType, path + ".")
        else:
            parts = path.split(".")
            alias = "_".join(parts[1:]) if len(parts) > 1 else path
            cols.append(F.col(path).alias(alias.replace(" ", "_")))
    return cols


@dlt.view(
    name="bronze_clean_{}".format(table_name),
    comment="Flattened CDC rows for {} feeding the silver merge".format(
        table_name
    ),
)
@dlt.expect_or_drop("valid_op", "op IS NOT NULL")
def bronze_clean():
    payload_df = dlt.read_stream("bronze_{}".format(table_name)).select(
        "payload.*"
    )
    # Deletes carry the row image in `before`; everything else in `after`.
    pre_merge_df = payload_df.withColumn(
        "row_selected",
        F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after")),
    )
    df = pre_merge_df.select(
        [
            F.col("op"),
            F.col("source.db").alias("db"),
            F.col("source.table").alias("table_ingest"),
            F.col("ts_ms"),
            F.col("source.pos").alias("source_pos"),
        ]
        + flatten_struct(pre_merge_df.select("row_selected").schema)
    )
    for name, logical in logical_types.items():
        template = LOGICAL_CONVERTERS.get(logical)
        if template and name in df.columns:
            df = df.withColumn(
                name, F.expr(template.format(c="`{}`".format(name)))
            )
    return df


dlt.create_streaming_table(
    name="{}_final".format(table_name),
    comment="Current-state {} rows merged from Debezium CDC "
    "(SCD type 1, deletes applied)".format(table_name),
    table_properties={"quality": "silver"},
)

# ts_ms has millisecond granularity, so ties are possible for rapid changes to
# the same row; the binlog position (source.pos) breaks them deterministically.
dlt.apply_changes(
    target="{}_final".format(table_name),
    source="bronze_clean_{}".format(table_name),
    keys=key_columns,
    sequence_by=F.struct(F.col("ts_ms"), F.col("source_pos")),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "db", "table_ingest", "ts_ms", "source_pos"],
)
