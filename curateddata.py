from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
import logging

class Curated:
    spark = SparkSession.builder.appName("").config('spark.jars.packages','net.snowflake:snowflake-jdbc:3.12.17,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').enableHiveSupport().getOrCreate()
    curated = spark.read.csv("C:\\Users\\gurudev.r\\Downloads\\cleansed_details_log.csv",header=True, inferSchema=True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def data_from_s3(self):
        spark = SparkSession.builder.appName(" ").config('spark.ui.port', '4050').config(
            "spark.master", "local").enableHiveSupport().getOrCreate()
        curated = spark.read.csv("C:\\Users\\gurudev.r\\Downloads\\cleansed_details_log.csv", header=True, inferSchema=True)
        curated.show()

        def split_date(val):
            return val.split(":")[0] + " " + val.split(":")[1]

        split_date_udf = udf(lambda x: split_date(x), StringType())
        replace = lambda x: sum(when(x, 1).otherwise(0))

        logs_per_device =  curated.drop("referrer") \
            .na.fill("Na") \
            .withColumn("day_hour", split_date_udf(col("datetime"))).groupBy("day_hour", "ip") \
            .agg(replace(col('method') == "PUT").alias("no_put"), \
                         replace(col('method') == "POST").alias("no_post"), \
                         replace(col('method') == "HEAD").alias("no_head"), \
                         ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
            .select("row_id", "day_hour", "ip", "no_put", "no_post", "no_head")
        logs_per_device.show()


        # logs_per_device.write.mode("overwrite").saveAsTable("log_agg_per_device")
        #logs_per_device.write.csv("",header = True)
        logs_across_device = logs_per_device.groupBy("day_hour") \
            .agg(count(col("ip")).alias("no_of_clients"), \
                 sum(col('no_put')).alias("no_put"), \
                 sum(col('no_post')).alias("no_post"), \
                 sum(col('no_head')).alias("no_head"), \
                 ).orderBy(asc("day_hour")).withColumn("row_id", monotonically_increasing_id()) \
            .select("row_id", "day_hour", "no_of_clients", "no_put", "no_post", "no_head")

        logs_across_device.show()


        sfOptions = {
                "sfURL": r"https://tm57257.europe-west4.gcp.snowflakecomputing.com/",
                "sfAccount": "tm57257",
                "sfUser": "TESTDATA",
                "sfPassword": "Welcome@1",
                "sfDatabase": "GURUDEV_DB",
                "sfSchema": "PUBLIC",
                "sfWarehouse": "COMPUTE_WH",
                "sfRole": "ACCOUNTADMIN"
            }

        curated.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                                       "{}".format(
                                                                                           r"curated_log_details")).mode(
                "overwrite").options(header=True).save()
        logs_per_device.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                      "{}".format(
                                                                          r"log_per_device_details")).mode(
            "overwrite").options(header=True).save()

        logs_across_device.write.format("snowflake").options(**sfOptions).option("dbtable",
                                                                      "{}".format(
                                                                          r"log_across_device_log_details")).mode(
            "overwrite").options(header=True).save()

if __name__ == '__main__':
    try:
        curated = Curated()
    except Exception as e:
        logging.error('Error at %s', 'Setup Object creation', exc_info=e)
        sys.exit(1)

    try:
        curated.data_from_s3()
    except Exception as e:
        logging.error('Error at %s', 'read from s3 clean', exc_info=e)
        sys.exit(1)


