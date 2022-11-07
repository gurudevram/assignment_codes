import sys
import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F
import logging
import re


class Setup:
    spark = SparkSession.builder.appName("finalassignment").config('spark.ui.port', '4050').config(
        "spark.master", "local").enableHiveSupport().getOrCreate()
    # curated_data = spark.read.csv('D:\\assigment_results\\saved_clensed_data.csv',header=True, inferSchema=True)


    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_curated_data(self):
        try:
            self.curated_data = self.spark.read.csv('D:\\assigment_results\\saved_clensed_data.csv',header=True, inferSchema=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.curated_data.printSchema()
            self.curated_data.show()


class Curated(Setup):

    def drop_referer(self):
        self.curated_data = self.curated_data.drop("referer")
        self.curated_data.show()

    # def update_to_s3(self):
    #     self.curated_data.write.csv("s3://finalassesment/curated_data/log_data_ip_request.csv", mode="overwrite", header=True)


class Aggr(Curated_Data):
    pass

    # check distinct no. of user device ie ip
    def check_unique_method(self):
        self.curated_data.select("method").distinct().show()

    def perform_agg(self):
        # add column hour,Get,Post,Head
        agg_logs = self.curated_data.withColumn("No_get", when(col("method(GET)") == "GET", "GET")) \
            .withColumn("No_post", when(col("method(GET)") == "POST", "POST")) \
            .withColumn("No_Head", when(col("method(GET)") == "HEAD", "HEAD")) \
            .withColumn("hour", hour(col("datetime")))
        agg_logs.show()

        #aggregation per device
        agg_logs_per_device = agg_logs.withColumn("day_hour", date_format(col("datetime"), "yyyy-MM-dd_hh")) \
            .groupBy(col("ip"), col("day_hour")) \
            .agg(collect_list(col("method")).alias("coll")) \
            .withColumn("no_get", size(filter(col("coll"), lambda x: x == "GET"))) \
            .withColumn("no_post", size(filter(col("coll"), lambda x: x == "POST"))) \
            .withColumn("no_head", size(filter(col("coll"), lambda x: x == "HEAD"))) \
            .select(col("ip"), col("day_hour"), col("no_get"), col("no_post"), col("no_head"))

        agg_logs_per_device.show()

        # perform aggregation across device
        df_agg_across_device = agg_logs.withColumn("day_hour", date_format(col("datetime"), "yyyy-MM-dd_hh")) \
            .groupBy(col("day_hour")) \
            .agg(collect_set(col("ip")).alias("clients"), collect_list(col("method")).alias("coll")) \
            .withColumn("no_of_clients", size(col("clients"))) \
            .withColumn("no_get", size(filter(col("coll"), lambda x: x == "GET"))) \
            .withColumn("no_post", size(filter(col("coll"), lambda x: x == "POST"))) \
            .withColumn("no_head", size(filter(col("coll"), lambda x: x == "HEAD"))) \
            .select(col("day_hour"), col("no_of_clients") , col("no_get"), col("no_post"), col("no_head")) \
            df_agg_across_device.show()

        # write to s3 curated-layer
        df_agg_per_device.write.csv("s3a://finalassesment/curated-layer/aggregation/per_device/", header=True,mode='overwrite')
        df_agg_across_device.write.csv("s3a://finalassesment/curated-layer/aggregation/across_device/",
                                       header=True,mode='overwrite')

if __name__ == '__main__':
    try:
        setup = Setup()
    except Exception as e:
        logging.error('Error at %s', 'Setup Object creation', exc_info=e)
        sys.exit(1)

    try:
        setup.read_from_clensed_data()
    except Exception as e:
        logging.error('Error at %s', 'read from s3 clean', exc_info=e)
        sys.exit(1)

    try:
        curated = Curated()
    except Exception as e:
        logging.error('Error at %s', 'curated Object creation', exc_info=e)
        sys.exit(1)

    try:
        curated_data.drop_referer()
    except Exception as e:
        logging.error('Error at %s', 'drop referer column', exc_info=e)
        sys.exit(1)

    try:
        curated.write_to_s3()
    except Exception as e:
        logging.error('Error at %s', 'write to s3', exc_info=e)
        sys.exit(1)

    try:
        agg = Agg()
    except Exception as e:
        logging.error('Error at %s', 'error creating Object agg', exc_info=e)
        sys.exit(1)

    try:
        agg.check_unique_user()
    except Exception as e:
        logging.error('Error at %s', 'check distinct user', exc_info=e)
        sys.exit(1)

    try:
        agg.perform_agg()
    except Exception as e:
        logging.error('Error at %s', 'perform_agg', exc_info=e)
        sys.exit(1)
