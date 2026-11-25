from pyspark.sql.types import ArrayType, StructField, StructType, StringType, LongType, BooleanType
from pyspark.sql import functions as F
from datetime import datetime, timezone

# Use a shared ingestion timestamp across all Bronze datasets
# to associate related records with the same pipeline snapshot.
ts = datetime.now(timezone.utc).isoformat()
ingestion_timestamp = F.to_timestamp(F.lit(ts))

line_schema = ArrayType(
    StructType([
        StructField("$type", StringType(), True),
        StructField("created", StringType(), True),
        StructField("crowding", StructType([
            StructField("$type", StringType(), True)
        ]), True),
        StructField("disruptions", ArrayType(StringType()), True),
        StructField("id", StringType(), True),
        StructField("lineStatuses", ArrayType(StringType()), True),
        StructField("modeName", StringType(), True),
        StructField("modified", StringType(), True),
        StructField("name", StringType(), True),
        StructField("routeSections", ArrayType(
            StructType([
                StructField("$type", StringType(), True),
                StructField("destination", StringType(), True),
                StructField("destinationName", StringType(), True),
                StructField("direction", StringType(), True),
                StructField("name", StringType(), True),
                StructField("originationName", StringType(), True),
                StructField("originator", StringType(), True),
                StructField("serviceType", StringType(), True),
                StructField("validFrom", StringType(), True),
                StructField("validTo", StringType(), True)
            ])
        ), True),
        StructField("serviceTypes", ArrayType(
            StructType([
                StructField("$type", StringType(), True),
                StructField("name", StringType(), True),
                StructField("uri", StringType(), True)
            ])
        ), True)
    ])
)

disruption_schema = ArrayType( 
                            StructType([
                            StructField('$type', StringType(), True), 
                            StructField('additionalInfo', StringType(), True), 
                            StructField('affectedRoutes', ArrayType(StringType(), True), True), 
                            StructField('affectedStops', ArrayType(StringType(), True), True), 
                            StructField('category', StringType(), True), 
                            StructField('categoryDescription', StringType(), True), 
                            StructField('created', StringType(), True), 
                            StructField('description', StringType(), True), 
                            StructField('lastUpdate', StringType(), True), 
                            StructField('line_id', StringType(), True), 
                            StructField('type', StringType(), True)]), True
                            )

status_schema = ArrayType(
                    StructType([
                                StructField('$type', StringType(), True), 
                                StructField('created', StringType(), True), 
                                StructField('crowding', 
                                            StructType([StructField('$type', StringType(), True)]), True), 
                                            StructField('disruptions', 
                                                        ArrayType(StringType(), True), True), 
                                                        StructField('id', StringType(), True), 
                                                        StructField('lineStatuses', 
                                                                    ArrayType(
                                                                        StructType(
                                                                            [StructField('$type', StringType(), True), StructField('created', StringType(), True), 
                                                                            StructField('disruption',
                                                                            StructType(
                                                                                [StructField('$type', StringType(), True), 
                                                                                StructField('additionalInfo', StringType(), True), 
                                                                                StructField('affectedRoutes', 
                                                                                ArrayType(StringType(), True), True), StructField('affectedStops', ArrayType(StringType(), True), True), 
                                                                                StructField('category', StringType(), True), 
                                                                                StructField('categoryDescription', StringType(), True), 
                                                                                StructField('created', StringType(), True), StructField('description', StringType(), True)]), True),
                                                        StructField('id', LongType(), True), 
                                                        StructField('lineId', StringType(), True), 
                                                        StructField('reason', StringType(), True), 
                                                        StructField('statusSeverity', LongType(), True), 
                                                        StructField('statusSeverityDescription', StringType(), True), 
                                                        StructField('validityPeriods', 
                                                                        ArrayType(StructType(
                                                                                    [StructField('$type', StringType(), True), 
                                                                                    StructField('fromDate', StringType(), True), StructField('isNow', BooleanType(), True), 
                                                                                    StructField('toDate', StringType(), True)]), True), True)]), True), True), 
                                                        StructField('modeName', StringType(), True), 
                                                        StructField('modified', StringType(), True), 
                                                        StructField('name', StringType(), True), 
                                                        StructField('routeSections', 
                                                                    ArrayType(StringType(), True), True), 
                                                        StructField('serviceTypes', 
                                                                    ArrayType(StructType(
                                                                        [StructField('$type', StringType(), True), StructField('name', StringType(), True), StructField('uri', StringType(), True)]), True), True)]), True
                    )