from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Metrics represent observed API status frequencies,
# not official TfL reliability or service downtime.
@dp.materialized_view(
    name="gold.line_reliability"
)
def gold_line_reliability():
    return (
        spark.read
                .table("tfl_pipeline.gold.line_status_daily")
                .withColumn(
                    "year_month",
                    F.date_format("observation_date", "yyyy-MM")
                )
                .withColumnRenamed("statusSeverity", "status_severity")
                
                .groupBy("line_id", "year_month")
                # Calculate monthly metrics from aggregated daily observations.
                # Severity 6 and 9 represent severe and minor delays.
                # Special Service (severity 0) is tracked separately.
                .agg(
                    F.sum("observation_count").alias("total_observations"),

                    F.sum(
                        F.when(
                            F.col("status_severity") == 10,
                            F.col("observation_count")
                        ).otherwise(0)
                    ).alias("good_service_observations"),

                    F.sum(
                        F.when(
                            F.col("status_severity") == 9,
                            F.col("observation_count")
                        ).otherwise(0)
                    ).alias("minor_delay_observations"),

                    F.sum(
                        F.when(
                            F.col("status_severity") == 6,
                            F.col("observation_count")
                        ).otherwise(0)
                    ).alias("severe_delay_observations"),

                    F.sum(
                        F.when(
                            F.col("status_severity") == 0,
                            F.col("observation_count")
                        ).otherwise(0)
                    ).alias("special_service_observations")
                )
                .withColumn(
                "delay_observations_pct",
                F.round(
                    (
                        F.col("minor_delay_observations") +
                        F.col("severe_delay_observations")
                    ) / F.col("total_observations") * 100,
                    2
                )
            )
                .withColumn(
                    "special_or_delay_pct",
                    F.round(
                        (
                            F.col("special_service_observations") +
                            F.col("minor_delay_observations") +
                            F.col("severe_delay_observations")
                        ) / F.col("total_observations") * 100,
                        2
                    )
                )
    )