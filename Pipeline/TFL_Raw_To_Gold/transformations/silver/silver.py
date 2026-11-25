from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, BooleanType
from silver_schemas import line_schema, disruption_schema, status_schema




# Create a temporary view as the common source for  Silver tables
@dp.view()
def tfl_lines_parsed():
    
    df = (
            spark.readStream
            .table("tfl_pipeline.bronze.line_data")
            .select("value", 
                    F.col("ingest_timestamp").cast("timestamp"))
            .withColumn("parsed", F.from_json("value", line_schema))
            .withColumn("json_row", F.explode("parsed"))
            .drop("parsed", "value")
        )
    
    return (df
            .select(
                    "json_row.id",
                    "json_row.name",
                    F.col("json_row.modeName").alias("mode"),
                    "json_row.disruptions",
                    "json_row.lineStatuses",
                    F.get(F.col("json_row.routeSections"), 0).alias("outbound"),
                    F.get(F.col("json_row.routeSections"), 1).alias("inbound"),
                    F.col("json_row.serviceTypes.name").alias("serviceTypes"),
                    F.col("json_row.created").alias("source_created_at"),
                    "ingest_timestamp"
                    )   
            )

# Create a temporary view as the common source for  Silver tables
@dp.view()
def tfl_status_parsed():
    df = (
            spark.readStream
            .table("tfl_pipeline.bronze.status_data")
            .select("value", 
                    F.col("ingest_timestamp").cast("timestamp"))
            .withColumn("parsed", F.from_json("value", status_schema))
            .withColumn("json_row", F.explode("parsed"))
            .drop("parsed", "value")
        )
    return (df
        .select(
            "json_row.id",
            F.get(F.col("json_row.lineStatuses"), 0).alias("primary_status"),
            F.explode_outer("json_row.lineStatuses").alias("secondary_status"),
            "json_row.modeName",
            "json_row.modified",
            "json_row.routeSections",
            "ingest_timestamp"
        )
    )

@dp.table(
    name = "tfl_pipeline.silver.disruptions"
)
@dp.expect_or_fail("line_id", "line_id is not null")
def silver_tfl_disruptions():
    
    df = (
            spark.readStream
            .table("tfl_pipeline.bronze.disruption_data")
            .select("value", 
                    F.col("ingest_timestamp").cast("timestamp"))
            .withColumn("parsed", F.from_json("value", disruption_schema))
            .withColumn("json_row", F.explode("parsed"))
            .drop("parsed", "value")
        )
    return (
        df
        .select(
                "json_row.line_id",
                "json_row.category",
                "json_row.categoryDescription",
                F.col("json_row.created").alias("data_created").cast("timestamp"),
                F.col("json_row.lastUpdate").alias("last_data_update").cast("timestamp"),
                F.col("json_row.description").alias("disruption_description"),
                "json_row.type",
                "ingest_timestamp"
                 )
    )



@dp.table(
    name = "tfl_pipeline.silver.rout_sections"
)
@dp.expect_or_fail("id", "id is not null")
def silver_tfl_rout_sections():
    return(
        spark.readStream
        .table("tfl_lines_parsed")
        .select(
                F.col("id").alias("line_id"),
                F.col("outbound.destination").alias("destination_id_out"),
                F.col("outbound.destinationName").alias("destinationName_out"),
                F.col("outbound.name").alias("name_out"),
                F.col("outbound.originationName").alias("originationName_out"),
                F.col("outbound.originator").alias("originator_id_out"),
                F.col("outbound.validFrom").cast("timestamp").alias("validFrom_out"),
                F.col("outbound.validTo").cast("timestamp").alias("validTo_out"),

                F.col("inbound.destination").alias("destination_id_in"),
                F.col("inbound.destinationName").alias("destinationName_in"),
                F.col("inbound.name").alias("name_in"),
                F.col("inbound.originationName").alias("originationName_in"),
                F.col("inbound.originator").alias("originator_id_in"),
                F.col("inbound.validFrom").cast("timestamp").alias("validFrom_in"),
                F.col("inbound.validTo").cast("timestamp").alias("validTo_in"),
                "ingest_timestamp"
                )
    )


# Separate primary and secondary service statuses while preserving
# multiple secondary status records for the same line and snapshot.
@dp.table(
    name = "tfl_pipeline.silver.line_status_primary"
)
@dp.expect_or_fail("id", "id is not null")
def silver_tfl_line_status_primary():
    return(
        spark.readStream
        .table("tfl_status_parsed")
        .select(
                F.col("id").alias("line_id"),
                "primary_status.disruption.category",
                "primary_status.disruption.categoryDescription",
                "primary_status.disruption.created",
                F.col("primary_status.disruption.description").alias("primary_status"),
                "primary_status.statusSeverity",
                "primary_status.statusSeverityDescription",
                F.explode_outer(F.col("primary_status.validityPeriods.fromDate")).alias("fromDate"),
                F.explode_outer(F.col("primary_status.validityPeriods.isNow")).alias("isNow"),
                F.explode_outer(F.col("primary_status.validityPeriods.toDate")).alias("toDate"),
                "modeName",
                "modified",
                "routeSections",
                "ingest_timestamp"
                )
        .dropDuplicates()
        )
                
    
@dp.table(
    name = "tfl_pipeline.silver.line_status_secondary"
)
@dp.expect_or_fail("id", "id is not null")
def silver_tfl_line_status_secondary():
    return(
            spark.readStream
            .table("tfl_status_parsed")
            .select(
                    F.col("id").alias("line_id"),
                    "secondary_status.disruption.category",
                    "secondary_status.disruption.categoryDescription",
                    "secondary_status.disruption.created",
                    F.col("primary_status.disruption.description").alias("primary_status"),
                    F.col("secondary_status.disruption.description").alias("secondary_status"),
                    "secondary_status.statusSeverity",
                    "secondary_status.statusSeverityDescription",
                    F.explode_outer(F.col("secondary_status.validityPeriods.fromDate")).alias("fromDate"),
                    F.explode_outer(F.col("secondary_status.validityPeriods.isNow")).alias("isNow"),
                    F.explode_outer(F.col("secondary_status.validityPeriods.toDate")).alias("toDate"),
                    "modeName",
                    "modified",
                    "ingest_timestamp"
                )
            .filter(F.col("primary_status") != F.col("secondary_status"))
            .drop("primary_status")
        )
            
    


@dp.table(
    name = "tfl_pipeline.silver.lines"
)
@dp.expect_or_fail("id", "id is not null")
def silver_tfl_lines():
    return(
        spark.readStream
        .table("tfl_lines_parsed")
        .select(
                "id", 
                "name", 
                "mode", 
                F.explode_outer(F.col("serviceTypes")).alias("serviceTypes"), 
                "source_created_at",
                "ingest_timestamp"
                )
            )