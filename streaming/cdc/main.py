"""
Production‑ready Spark Structured Streaming job that ingests Debezium CDC
Parquet files for *bookings* from GCS, upserts & deletes them into a Delta Lake
 table stored on GCS, stamps audit columns, and archives processed files after
 each successful micro‑batch commit.

All variable and function names have been expanded to be self‑describing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, MapType
)
from delta.tables import DeltaTable
from datetime import datetime, timezone
import logging

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — GCS locations & stream tuning
# ─────────────────────────────────────────────────────────────────────────────
DELTA_BOOKING_TABLE_NAME          = "booking"       # Metastore table name (optional)
MICRO_BATCH_INTERVAL              = "10 seconds"    # Trigger cadence
MAX_FILES_PER_TRIGGER             = 100             # Throttle file discovery per batch

GCS_RAW_BOOKINGS_CDC_PATH         = "gs://corp-streaming-data/v3-databases/landzone/landing-processing/{DELTA_BOOKING_TABLE_NAME}"
GCS_ARCHIVE_BOOKINGS_CDC_PATH     = "gs://corp-streaming-data/v3-databases/landzone/landing-processed/{DELTA_BOOKING_TABLE_NAME}"     
GCS_CHECKPOINT_BOOKING_CDC_PATH   = "gs://corp-streaming-data/v3-databases/landzone/landing-checkpoints/{DELTA_BOOKING_TABLE_NAME}"  
GCS_DELTA_BOOKING_TABLE_PATH      = "gs://corp-data-lakehouse/databases/v3-corp-app-prod-booking/main-booking/{DELTA_BOOKING_TABLE_NAME}"       

# ─────────────────────────────────────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("booking_cdc_stream")

# ─────────────────────────────────────────────────────────────────────────────
# Spark session initialisation
# ─────────────────────────────────────────────────────────────────────────────
spark_session = (
    SparkSession.builder
        .appName("BookingCDC_GCS_Delta_Stream")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        .getOrCreate()
)

spark_session.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# CDC envelope & row schema definitions
# ─────────────────────────────────────────────────────────────────────────────
booking_row_schema = StructType([
    StructField("id",               LongType(),   True),
    StructField("old_id",           IntegerType(), True),
    StructField("organization_id",  StringType(), True),
    StructField("origin_id",        StringType(), False),
    StructField("protocol",         StringType(), True),
    StructField("status",           StringType(), True),
    StructField("user_id",          StringType(), False),
    StructField("created_at",       LongType(),   True),   # Epoch‑ms
    StructField("updated_at",       LongType(),   True),
])

cdc_envelope_schema = StructType([
    StructField("before",      booking_row_schema, True),
    StructField("after",       booking_row_schema, True),
    StructField("op",          StringType(), True),
    StructField("ts_ms",       LongType(),  True),
    StructField("source",      MapType(StringType(), StringType()), True),
    StructField("transaction", MapType(StringType(), StringType()), True),
])

# ─────────────────────────────────────────────────────────────────────────────
# 1. Read Parquet CDC stream from GCS
# ─────────────────────────────────────────────────────────────────────────────
bookings_cdc_raw_df = (
    spark_session.readStream
        .schema(cdc_envelope_schema)
        .format("parquet")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .option("cleanSource", "archive")
        .option("sourceArchiveDir", GCS_ARCHIVE_BOOKINGS_CDC_PATH) 
        .load(GCS_RAW_BOOKINGS_CDC_PATH)
)

bookings_cdc_events_df = bookings_cdc_raw_df.select("op", "ts_ms", "before", "after")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Batch‑level merge logic
# ─────────────────────────────────────────────────────────────────────────────

def upsert_and_delete_micro_batch(micro_batch_df: DataFrame, micro_batch_id: int):

    """Apply upserts and deletes for a single micro‑batch, then archive files."""
    if micro_batch_df.isEmpty():
        log.info("Micro‑batch %s empty — nothing to process", micro_batch_id)
        return

    batch_processing_timestamp = datetime.now(timezone.utc)
    log.info("Processing micro‑batch %s at %s", micro_batch_id, batch_processing_timestamp)

    # Split delete and upsert events inside the batch
    delete_events_df = (
        micro_batch_df
            .filter(col("op") == "d")
            .select(col("before.id").alias("id"))
    )

    upsert_events_df = (
        micro_batch_df
            .filter(col("op") != "d")
            .select(
                col("after.id").alias("id"),
                col("after.old_id").alias("old_id"),
                col("after.user_id").alias("user_id"),
                col("after.organization_id").alias("organization_id"),
                col("after.protocol").alias("protocol"),
                col("after.status").alias("status"),
                col("after.origin_id").alias("origin_id"),
                expr("to_timestamp(after.created_at/1000)").alias("created_at"),
                expr("to_timestamp(after.updated_at/1000)").alias("updated_at"),
                lit(batch_processing_timestamp).alias("delta_inserted_at"),
                lit(batch_processing_timestamp).alias("delta_updated_at")
            )
    )

    # Ensure Delta table exists
    if not DeltaTable.isDeltaTable(spark_session, GCS_DELTA_BOOKING_TABLE_PATH):
        log.info("Creating Delta table at %s", GCS_DELTA_BOOKING_TABLE_PATH)
        (upsert_events_df.write
            .format("delta")
            .mode("overwrite")
            .save(GCS_DELTA_BOOKING_TABLE_PATH))
        try:
            spark_session.sql(
                f"CREATE TABLE IF NOT EXISTS {DELTA_BOOKING_TABLE_NAME} "
                f"USING DELTA LOCATION '{GCS_DELTA_BOOKING_TABLE_PATH}'")
        except Exception as exc:
            log.warning("Could not register table in metastore: %s", exc)

    booking_delta_table = DeltaTable.forPath(spark_session, GCS_DELTA_BOOKING_TABLE_PATH)

    # Upsert logic — keep delta_inserted_at immutable
    update_assignments = {
        "user_id":           "source.user_id",
        "organization_id":   "source.organization_id",
        "protocol":          "source.protocol",
        "status":            "source.status",
        "origin_id":         "source.origin_id",
        "created_at":        "source.created_at",
        "updated_at":        "source.updated_at",
        "delta_updated_at":  "source.delta_updated_at"
    }

    (booking_delta_table.alias("target")
        .merge(upsert_events_df.alias("source"), "target.id = source.id")
        .whenMatchedUpdate(set=update_assignments)
        .whenNotMatchedInsertAll()
        .execute())

    # Delete logic (if delete events present)
    if not delete_events_df.isEmpty():
        (booking_delta_table.alias("target")
            .merge(delete_events_df.alias("source"), "target.id = source.id")
            .whenMatchedDelete()
            .execute())

# ─────────────────────────────────────────────────────────────────────────────
# 3. Start the streaming query
# ─────────────────────────────────────────────────────────────────────────────
streaming_query = (
    bookings_cdc_events_df.writeStream
    .foreachBatch(upsert_and_delete_micro_batch)
    .outputMode("update")
    .option("checkpointLocation", GCS_CHECKPOINT_BOOKING_CDC_PATH)
    .trigger(processingTime=MICRO_BATCH_INTERVAL)
    .start()
)

streaming_query.awaitTermination()
