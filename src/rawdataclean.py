from pyspark.sql import *
from pyspark.sql.functions import *
import logging
import pyspark.sql.functions as F

class Initialize:
    spark = SparkSession.builder.master("local[1]").appName("").enableHiveSupport().getOrCreate()
    raw_data = spark.read.option("header", "false").option("delimiter", " ").csv(r"C:\Users\gurudev.r\Downloads\logdatafile_2.txt")


    def __init__(self):
        sc = self.spark.sparkContext
        sc.setLogLevel("Error")

    def read_from_raw_file(self):
        try:
            self.raw_data = self.spark.read.option("delimiter", " ").csv("C:\Users\gurudev.r\Downloads\logdatafile_2.txt")
            self.raw_data.show()

        except Exception as err:
             logging.error('Exception was thrown in connection %s' % err)
             print("Error is {}".format(err))
             sys.exit(1)

        else:
            self.df.printSchema()

    def get_columns(self):
        self.raw_data = self.raw_data.select(
            monotonically_increasing_id().alias('row_id'),
            col("_c0").alias("ip"),
            split(col("_c3"), " ").getItem(0).alias("datetime"),
            split(col("_c5"), " ").getItem(0).alias("method"),
            split(col("_c5"), " ").getItem(1).alias("request"),
            col("_c6").alias("status_code"),
            col("_c7").alias("size"),
            col("_c8").alias("referrer"),
            split(col("_c9"), " ").getItem(1).alias("user_agent"))
        self.raw_data.show(truncate = False)

    def clean_columns(self):
        # Remove any special characters in the request column(% ,- ? =)
        self.raw_data = self.raw_data.withColumn('datetime', regexp_replace('datetime', '\[|\]|', ''))

        self.raw_data.show(truncate = False)


    def write_to_hive(self):
        pass
        # **************************
        #self.df.write.csv("   ", mode="append", header=True)
        #self.df.write.saveAsTable('raw_log_details')


if __name__ == "__main__":
    # Start
    init = Initialize()
    try:
        init.read_from_s3()
    except Exception as e:
        logging.error('Error at %s', 'Reading from S3 Sink', exc_info=e)
        sys.exit(1)

    try:
        init.extract_columns()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)

    try:
        init.remove_character()
    except Exception as e:
        logging.error('Error at %s', 'extract_column_regex', exc_info=e)
        sys.exit(1)


    # try:
    #    init.write_to_csv()
    #   logging.info("Writing to Raw Layer S3 Successfull!")
    #except Exception as e:
    #   logging.error('Error at %s', 'write_to_s3', exc_info=e)
    #    sys.exit(1)

    try:
      init.write_to_hive()
    except Exception as e:
      logging.error('Error at %s', 'write to hive', exc_info=e)


