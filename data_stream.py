import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date

# Create a schema for incoming resources

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

@psf.udf(StringType())
def udf_convert_time(timestamp):
    d = parse_date(timestamp)
    return str(d.strftime('%y%m%d%H'))


def run_spark_job(spark):

   
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    # test being my topic

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 100) \    
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS")) \
        .select("SERVICE_CALLS.*")

    distinct_table = service_table \
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))

    
    counts_df = distinct_table \
        .withWatermark("call_datetime", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_datetime, "10 minutes", "5 minutes"),
            distinct_table.original_crime_type_name
            ).count()

   
    converted_df = counts_df.withColumn(
        "call_date_time", udf_convert_time(counts_df.call_date_time))

    
    calls_per_2_days = converted_df \
        .groupBy(
        psf.window(converted_df.call_date_time, "2 day")
    ).agg(psf.count("crime_id").alias("calls_per_2_day")).select("calls_per_2_day")

    # TODO write output stream
    query = counts_df \
        .writeStream \
        .outputMode('Complete') \
        .format('console') \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    # spark =
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
