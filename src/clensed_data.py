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
    spark = SparkSession.builder.appName("finalassignment").config('spark.ui.port', '4050').config("spark.master","local").enableHiveSupport().getOrCreate()
    clensed_data = spark.read.csv("C:\\Users\\gurudev.r\\Downloads\\logdatafile_2.txt")

    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_clensed_data(self):
        try:
            self.clensed_data = self.spark.read.csv(r"C:\\Users\\gurudev.r\\Downloads\\logdatafile_2.txt",header=True)
        except Exception as err:
            logging.error('Exception was thrown in connection %s' % err)
            print("Error is {}".format(err))
            sys.exit(1)
        else:
            self.clensed_data.printSchema()
            # self.clensed_data.show()

    def size_to_kb(self):
        self.clensed_data = self.clensed_data.withColumn('size', round(self.clensed_data.size / 1024, 2))
        return self.clensed_data

    def update_empty_string_with_null(self):
        self.clensed_data = self.clensed_data.select(
            [when(col(c) == "-", None).otherwise(col(c)).alias(c) for c in self.clensed_data.columns])
        self.clensed_data.show()
        return self.clensed_data


class CleansedLayer(Setup):

    def datetime_formatter(self):
        self.clensed_data = self.withColumn('datetime',to_timestamp('datetime','dd/MMM/yyyy:HH:mm:ss'))                                                                         .withColumn("datetime",to_timestamp("datetime",'MMM/dd/yyyy:hh:mm:ss'))
        # self.clensed_data.show()

    def referer_present(self):
        self.clensed_data = self.withColumn('referrer', when(col('referrer') == '-', "No").otherwise("Yes"))

        # clensed_data.write.format("csv").mode("overwrite").saveAsTable("save_clensed_data")

    # def update_to_s3(self):
    # self.clensed_data.write.csv("s3://finalassesment/clensed_data/log_data_ip_request.csv", mode="append",header=True)

    # def update_to_hive(self):
    #     pass
        # **************************
        # self.clensed_data.write.saveAsTable('clensedtable')

    def save_to_folder(self):
        self.raw_data.write.mode("overwrite").csv('D:\\assigment_results\\saved_clensed_data.csv',header=True)


if __name__ == "__main__":
    #SetUp
    setup = Setup()
    try:
        setup.read_from_clensed_data()
    except Exception as e:
        logging.error('Error at %s', 'Fetching data from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        setup.size_to_kb()
    except Exception as e:
        logging.error('Error at %s', 'size_to_kb', exc_info=e)
        sys.exit(1)

    try:
        setup.update_empty_string_with_null()
    except Exception as e:
        logging.error('Error at %s', 'update empty string with null', exc_info=e)
        sys.exit(1)

    #Clensed
    try:
        clean = CleansedLayer()
    except Exception as e:
        logging.error('Error at %s', 'Error at CleansedLayer Object Creation', exc_info=e)
        sys.exit(1)

    try:
        clean.datetime_formatter()
    except Exception as e:
        logging.error('Error at %s', 'Error at datetime formatter', exc_info=e)
        sys.exit(1)

    try:
        clean.referer_present()
    except Exception as e:
        logging.error('Error at %s', 'Error at referer present', exc_info=e)
        sys.exit(1)

    # try:
    #     clean.update_to_s3()
    #     logging.info("Writing to Clean Layer S3 Successfull!")
    # except Exception as e:
    #     logging.error('Error at %s', 'update_to_s3', exc_info=e)
    #     sys.exit(1)

    try:
        setup.save_to_folder()
    except Exception as e:
        logging.error('Error at %s', 'saving to clensed data to folder', exc_info=e)
        sys.exit(1)



