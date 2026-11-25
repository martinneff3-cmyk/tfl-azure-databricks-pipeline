from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import TimeType
# Use a shared ingestion timestamp across all Bronze datasets
# to associate related records with the same pipeline snapshot.
from transformations.silver.silver_schemas import ingestion_timestamp

# Store each raw JSON line data file as a single row of text in the Bronze table
@dp.table(
    name = "tfl_pipeline.bronze.line_data"
)
def bronze_line_data():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", True)
        .load("abfss://project1try@azuredatabricksmart.dfs.core.windows.net/api_data/line_data")
        .select(
            F.col("value"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.col("_metadata.file_modification_time").cast("timestamp").alias("source_file_modification_time"),
            ingestion_timestamp.alias("ingest_timestamp")
        )
    )

# Store each raw JSON disruption data file as a single row of text in the Bronze table
@dp.table(
    name = "tfl_pipeline.bronze.disruption_data"
)
def bronze_disruption_data():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", True)
        .load("abfss://project1try@azuredatabricksmart.dfs.core.windows.net/api_data/disruption_data")
        .select(
            F.col("value"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.col("_metadata.file_modification_time").cast("timestamp").alias("source_file_modification_time"),
            ingestion_timestamp.alias("ingest_timestamp")
        )
    )

@dp.table(
    name = "tfl_pipeline.bronze.status_data"
)
def bronze_status_data():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholeText", True)
        .load("abfss://project1try@azuredatabricksmart.dfs.core.windows.net/api_data/status_data")
        .select(
            F.col("value"),
            F.col("_metadata.file_path").alias("source_file_path"),
            F.col("_metadata.file_modification_time").cast("timestamp").alias("source_file_modification_time"),
            ingestion_timestamp.alias("ingest_timestamp")
        )
    )

