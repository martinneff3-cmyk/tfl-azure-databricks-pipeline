from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name = "tfl_pipeline.gold.line_status_daily"
)
def gold_status_daily():
    return (
        spark.read
        .table("tfl_pipeline.silver.line_status_primary")
        .withColumn(
            "observation_date",
            F.to_date("ingest_timestamp")
        )
        # Aggregate status observations by line, date, and primary status.
        # Repeated observations do not necessarily represent distinct disruptions.
        .groupBy(
            "line_id",
            "observation_date",
            "statusSeverityDescription",
            "statusSeverity"
        )
        .agg(
            F.count("*").alias("observation_count"),
            F.min("ingest_timestamp").alias("first_observed_at"),
            F.max("ingest_timestamp").alias("last_observed_at")
        )
        .fillna("No primary status")
        )

