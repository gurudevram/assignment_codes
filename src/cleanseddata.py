from pyspark.sql import *
from pyspark.sql.functions import *
import logging


class Clean:
    spark = SparkSession.builder.appName("").config('spark.jars.packages', 'net.snowflake:snowflake-jdbc:3.12.17,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.1').enableHiveSupport().getOrCreate()
    clean_data = spark.read.csv("C:\\Users\\gurudev.r\\Downloads\\raw_details_log.csv",header = True)

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_raw(self):
        try:
            self.clean_data = self.spark.read.csv("C:\\Users\\gurudev.r\\Downloads\\raw_details_log.csv",header = True)
            self.clean_data.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.clean_data.printSchema()

# raw_clean_data.show(10, truncate=False)


    def cleansed_data_log(self):
        self.clean_data = self.clean_data.withColumn('datetime',to_timestamp('datetime','dd/MMM/yyyy:HH:mm:ss'))\
                      .withColumn("request", regexp_replace("request", "[@\+\#\$\%\^\!\-\,]+", "")) \
                      .withColumn('status_code', col('status_code').cast('int')) \
                      .withColumn('size', col('size').cast('int')) \
                      .withColumn("referrer", regexp_replace("referrer", "-", "Null")) \
                      .withColumn('referrer_present', when(col('referrer') == 'Null', "N").otherwise("Y")) \
                      .withColumn('size', round(col("size")/ 1000)).withColumnRenamed("size", "size_into_KB") \
                      .withColumn('method', regexp_replace('method', 'GET', 'PUT'))\
                      .withColumn('datetime',date_format(col("datetime"), "MM-dd-yyyy:HH:mm:ss"))
        self.clean_data.show(truncate=False)


    def write_to_s3(self):
        self.clean_data.write.csv("   ", mode="append", header=True)

    def write_to_hive(self):
        pass
        # **************************
        #self.df.write.csv("  ", mode="append", header=True)
        # self.clean_data.write.saveAsTable('cleanse_log_details')

    def connect_to_snowflake(self):
        self.sfOptions = {
            "sfURL": r"",
            "sfAccount": "",
            "sfUser": "",
            "sfPassword": "",
            "sfDatabase": "GURUDEV_DB",
            "sfSchema": "PUBLIC",
            "sfWarehouse": "COMPUTE_WH",
            "sfRole": "ACCOUNTADMIN"
        }

        self.clean_data.write.format("snowflake").options(**self.sfOptions).option("dbtable",
                                                                           "{}".format(r"clensed_log_details")).mode(
            "overwrite").options(header=True).save()

if __name__ == "__main__":
    # Start
    clean = Clean()

    try:
        clean.read_from_raw()
    except Exception as e:
        logging.error('Error at %s', 'read_from_raw', exc_info=e)
        sys.exit(1)

    try:
        clean.cleansed_data_log()
    except Exception as e:
        logging.error('Error at %s', 'cleansed_data_log', exc_info=e)
        sys.exit(1)

    # try:
    #     clean.write_to_hive()
    # except Exception as e:
    #     logging.error('Error at %s', 'write_to_hive', exc_info=e)
    #     sys.exit(1)

    try:
        clean.connect_to_snowflake()
    except Exception as e:
        logging.error('Error at %s', 'snoflake in clensed', exc_info=e)
