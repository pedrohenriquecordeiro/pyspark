
# ──────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────
import logging , os

from py4j.java_gateway import java_import
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from schemas import schemas  # external dictionary with schemas

# ──────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────
RAW_BASE_PATH_CDC_TOPICS    = "gs://corp-streaming-data/cdc/debezium/v3/corp-app-prod/landzone/landing-processing/topics"
ARCHIVE_BASE_PATH_PROCESSED = "gs://corp-streaming-data/cdc/debezium/v3/corp-app-prod/landzone/landing-processed"
DELTA_LAKE_BASE_PATH        = "gs://corp-data-lakehouse/databases/v3/corp-app-prod"

# ──────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("process")

# ──────────────────────────────────────────────────────────
# Spark Session (single, reused)
# ──────────────────────────────────────────────────────────
spark = (
    SparkSession
    .builder
    .appName("process")

    # Kubernetes settings
    .config("spark.kubernetes.executorEnv.PYTHONUNBUFFERED", "1")
    .config("spark.kubernetes.executorEnv.LOG_LEVEL", "INFO")

    # Parquet datetime rebase
    .config("spark.sql.parquet.datetimeRebaseModeInRead",  "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    # Performance / AQE tuning
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.debug.maxToStringFields", 2000)
    .config("spark.sql.shuffle.partitions", "200")                 # baseline; AQE will shrink
    .config("spark.sql.files.maxPartitionBytes", "134217728")      # 128 MB
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600")   # 100 MB

    # Vectorized Parquet reader
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")

    # Delta
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")

    # GCS connector tweaks
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.gs.status.parallel.enable", "true")
    .config("spark.hadoop.fs.gs.inputstream.support.gzip.encoding", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # Set Spark log level to WARN

# JVM imports (do once on driver)
java_import(spark._jvm, "org.apache.hadoop.fs.FileUtil")
java_import(spark._jvm, "java.net.URI")

# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────

def build_upsert_column_expressions(incoming_after_columns_list, target_table_schema, primary_key_columns_list):
    """Create the list of Columns for .select(); cast PKs to string and fill missing cols with NULL."""
    column_expressions_list = []
    for struct_field in target_table_schema:
        is_primary_key = struct_field.name in primary_key_columns_list
        if struct_field.name in incoming_after_columns_list:
            column_expression = F.col(f"after.{struct_field.name}")
        else:
            column_expression = F.lit(None)
        column_expression = column_expression.cast("string" if is_primary_key else struct_field.dataType)
        column_expressions_list.append(column_expression.alias(struct_field.name))
    return column_expressions_list

def deduplicate_change_events(source_dataframe, primary_key_columns_list):
    """Keep only the latest event per primary key. Deletes win over updates when timestamps tie."""

    logger.info(f"Deduplicating source DataFrame with {len(primary_key_columns_list)} primary key(s).")
    order_columns = [F.col("cdc_ts").desc_nulls_last(), F.col("cdc_op").desc()]  # 'd' > 'u'
    window_spec = Window.partitionBy(*primary_key_columns_list).orderBy(*order_columns)
    return (
        source_dataframe
            .withColumn("row_number_deduplicate", F.row_number().over(window_spec))
            .filter(F.col("row_number_deduplicate") == 1)
            .drop("row_number_deduplicate")
    )

# ──────────────────────────────────────────────────────────
# Core processing
# ──────────────────────────────────────────────────────────

def merge_batch_for_logical_table(logical_table_name: str, raw_batch_path: str) -> bool:
    """
    Read one Debezium folder, build upsert/delete DataFrames, deduplicate per PK, and execute a Delta MERGE.
    Returns True if the merge succeeded, else False.
    """
    logger.info(f"[{logical_table_name}] Reading CDC batch: {raw_batch_path}")

    raw_cdc_dataframe = (
        spark.read
             .option("mergeSchema", "true")
             .parquet(raw_batch_path)
    )

    # Sanity checks
    if {"op", "after", "before"}.difference(raw_cdc_dataframe.columns):
        logger.warning(f"[{logical_table_name}] Missing Debezium columns, skipping")
        return False

    if raw_cdc_dataframe.rdd.isEmpty():
        logger.info(f"[{logical_table_name}] Empty batch, skipping")
        return False

    # Gather schema info
    target_table_schema          = schemas[logical_table_name]["schema"]
    primary_key_columns_list     = schemas[logical_table_name]["primary_keys"]
    incoming_after_columns_list  = raw_cdc_dataframe.select("after.*").columns

    # Build source DataFrames (attach metadata BEFORE union)
    upserts_dataframe = (
        raw_cdc_dataframe
            .filter(F.col("op") != "d")
            .withColumn("cdc_op", F.lit("u"))
            .withColumn("cdc_ts", F.col("ts_ms"))  # adjust if your timestamp column differs
            .select(*build_upsert_column_expressions(incoming_after_columns_list, target_table_schema, primary_key_columns_list),
                    "cdc_op", "cdc_ts")
            .withColumn("delta_deleted", F.lit(False))
    )

    deletes_dataframe = (
        raw_cdc_dataframe
            .filter(F.col("op") == "d")
            .withColumn("cdc_op", F.lit("d"))
            .withColumn("cdc_ts", F.col("ts_ms"))
            .select(*build_upsert_column_expressions(incoming_after_columns_list, target_table_schema, primary_key_columns_list),
                    "cdc_op", "cdc_ts")
            .withColumn("delta_deleted", F.lit(True))
    )

    change_events_source_dataframe = (
        upserts_dataframe
            .unionByName(deletes_dataframe, allowMissingColumns=True)
            .withColumn("delta_updated_at", F.current_timestamp())
    )

    # Deduplicate to ensure single row per PK for MERGE
    deduped_source_dataframe = deduplicate_change_events(change_events_source_dataframe, primary_key_columns_list)

    data_columns_for_merge_list = [sf.name for sf in target_table_schema] + ["delta_updated_at"]

    # Delta table path/name
    delta_table_path = f"{DELTA_LAKE_BASE_PATH}/{logical_table_name}"
    safe_table_name  = logical_table_name.replace(".", "_").replace("-", "_")

    # Create table if missing
    if not DeltaTable.isDeltaTable(spark, delta_table_path):
        logger.info(f"Creating Delta table for `{safe_table_name}` at {delta_table_path}")
        (
            deduped_source_dataframe
                .filter("delta_deleted = false")
                .select(*data_columns_for_merge_list)
                .limit(0)
                .write
                .format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .save(delta_table_path)
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {safe_table_name} USING DELTA LOCATION '{delta_table_path}'")

    merge_condition_sql = " AND ".join([f"target.{primary_key} = source.{primary_key}" for primary_key in primary_key_columns_list])

    logger.info(f"[{logical_table_name}] Running MERGE with condition: {merge_condition_sql}")
    delta_table = DeltaTable.forPath(spark, delta_table_path)
    (
        delta_table.alias("target")
                   .merge(deduped_source_dataframe.alias("source"), merge_condition_sql)
                   .whenMatchedUpdate(
                       condition="NOT source.delta_deleted",
                       set={column: f"source.{column}" for column in data_columns_for_merge_list}
                   )
                   .whenNotMatchedInsert(
                       condition="NOT source.delta_deleted",
                       values={column: f"source.{column}" for column in data_columns_for_merge_list}
                   )
                   .whenMatchedDelete(condition="source.delta_deleted")
                   .execute()
    )

    # Optional maintenance – consider moving to a separate job or making it conditional
    # delta_table.optimize().executeCompaction()
    # delta_table.vacuum(retentionHours=VACUUM_RETENTION_HOURS)

    logger.info(f"[{logical_table_name}] Merge completed.")
    return True


def archive_raw_batch_to_processed_area(logical_table_name: str , raw_batch_gcs_path: str):
    """Copy raw files to the archive location and delete originals. Runs on the driver."""
    hadoop_configuration_object = spark.sparkContext._jsc.hadoopConfiguration()
    jURI        = spark._jvm.java.net.URI
    jPath       = spark._jvm.org.apache.hadoop.fs.Path
    jFileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
    jFileUtil   = spark._jvm.org.apache.hadoop.fs.FileUtil

    source_uri_object      = jURI(raw_batch_gcs_path)
    destination_uri_object = jURI(f"{ARCHIVE_BASE_PATH_PROCESSED}/{logical_table_name}")

    source_filesystem      = jFileSystem.get(source_uri_object, hadoop_configuration_object)
    destination_filesystem = jFileSystem.get(destination_uri_object, hadoop_configuration_object)

    source_hdfs_path       = jPath(source_uri_object.getPath())
    destination_hdfs_path  = jPath(destination_uri_object.getPath())

    copy_operation_succeeded = jFileUtil.copy(
        source_filesystem, source_hdfs_path,
        destination_filesystem, destination_hdfs_path,
        False,   # do not delete source during copy
        True,    # overwrite
        hadoop_configuration_object
    )
    if copy_operation_succeeded:
        source_filesystem.delete(source_hdfs_path, True)
        logger.info(f"[{logical_table_name}] Archived -> {destination_hdfs_path}")
    else:
        logger.error(f"[{logical_table_name}] Archive FAILED for {source_hdfs_path}")


# ──────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    hadoop_conf   = spark.sparkContext._jsc.hadoopConfiguration()
    raw_uri       = spark._jvm.java.net.URI(RAW_BASE_PATH_CDC_TOPICS)
    hadoop_fs     = spark._jvm.org.apache.hadoop.fs.FileSystem.get(raw_uri, hadoop_conf)
    raw_root_path = spark._jvm.org.apache.hadoop.fs.Path(raw_uri.getPath())

    for path in hadoop_fs.listStatus(raw_root_path):

        if not path.isDirectory():
            logger.warning(f"Skipping non-directory: {path.getPath()}")
            continue

        folder_path = path.getPath().toString()
        table_name = path.getPath().getName()

        if table_name not in schemas:
            logger.warning(f"No schema defined for `{table_name}`, skipping")
            continue

        # keep only topics that start with the desired prefix
        if not table_name.startswith(os.getenv("PATH_PREFIX")):
            continue

        logger.info(f"Processing folder: {folder_path}")
        try:
            if merge_batch_for_logical_table(table_name, folder_path):
                archive_raw_batch_to_processed_area(table_name, folder_path)
        except Exception as exception:
            logger.exception("[%s] Merge failed: %s", table_name, exception)

        logger.info(f"Finished processing `{table_name}`")

    logger.info("All folders processed.")
    spark.stop()
