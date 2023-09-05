import os
import numpy as np


import timeit
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# spark = SparkSession.builder.appName("whitehouse").getOrCreate()
spark = SparkSession.builder.master("spark://192.168.1.112:7077").getOrCreate()

os.system("mkdir data")
os.system("wget -c -q -i links.txt -P data")

# we ignore the time of loading the CSVs and data pre-processing


def create_pyspark_df():
    schema = StructType([
        StructField('Last Name', StringType(), True),
        StructField('First Name', StringType(), True),
        StructField('Middle Initial', StringType(), True),
        StructField('UIN', StringType(), True),
        StructField('BDGNBR', StringType(), True),
        StructField('Access Type', StringType(), True),
        StructField('TOA', StringType(), True),
        StructField('POA', StringType(), True),
        StructField('TOD', StringType(), True),
        StructField('POD', StringType(), True),
        StructField('Appointment Made Date', StringType(), True),
        StructField('Appointment Start Date', StringType(), True),
        StructField('Appointment End Date', StringType(), True),
        StructField('Appointment Cancel Date', StringType(), True),
        StructField('Total People', IntegerType(), True),
        StructField('Last Updated By', StringType(), True),
        StructField('POST', StringType(), True),
        StructField('Last Entry Date', StringType(), True),
        StructField('Terminal Suffix', StringType(), True),
        StructField('Visitee Last Name', StringType(), True),
        StructField('Visitee First Name', StringType(), True),
        StructField('Meeting Location', StringType(), True),
        StructField('Meeting Room', StringType(), True),
        StructField('Caller Last Name', StringType(), True),
        StructField('Caller First Name', StringType(), True),
        StructField('CALLER_ROOM', StringType(), True),
        StructField('RELEASEDATE', StringType(), True)
    ])
    df = spark.read.csv("data/*.csv", header=True,
                        schema=schema, enforceSchema=True)
    timestamp_cols = ["Appointment Made Date", "Appointment Start Date",
                      "Appointment End Date", "Appointment Cancel Date", "Last Entry Date"]
    for ts_col in timestamp_cols:
        df = df.withColumn(ts_col, F.to_timestamp(ts_col, "M/d/yyyy H:mm"))
    df = df.withColumn("TOA", F.to_timestamp("TOA", "MMM [ ]d yyyy [ ]h:mma"))
    return df


def pyspark_test_suite(df):
    # 10 most frequent visitors
    df.groupBy(
        "First Name", "Last Name", "Middle Initial",
    ).agg(
        F.count("Last Name").alias("count")
    ).orderBy(F.col("count").desc()).take(10)

    # The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    df.groupBy("Visitee First Name", "Visitee Last Name").agg(
        F.count("Last Name").alias("count")
    ).orderBy(F.col("count").desc()).take(10)

    # The 10 most frequent visitor-visitee combinations.
    df.groupBy(
        "First Name", "Middle Initial", "Last Name",
        "Visitee First Name", "Visitee Last Name").agg(
        F.count("Last Name").alias("count")
    ).orderBy(F.col("count").desc()).take(10)

    # lateness
    df.filter(F.col("TOA").isNotNull()).select(
        ((F.col("TOA").cast("long") -
         F.col("Appointment Start Date").cast("long"))/60).alias("delta")
    ).rdd.flatMap(lambda x: x).histogram(list(np.linspace(-80, +120, 50)))

    # duration
    df.select(
        ((F.col("Appointment End Date").cast("long") -
         F.col("Appointment Start Date").cast("long")) / 60).alias("delta")
    ).rdd.flatMap(lambda x: x).histogram(list(np.linspace(0, +1200, 100)))

    # total people
    df.select(
        (F.col("Total People"))
    ).rdd.flatMap(lambda x: x).histogram(list(np.linspace(0, 200, 50)))


print("Loading spark dataframe")
spark_df = create_pyspark_df().cache()
pyspark_test_suite(spark_df)  # force spark to actually load the data

if __name__ == "__main__":
    print("started scripts")
    multiplier = [1, 2, 5, 10, 20]
    N = spark_df.count()
    pyspark_times = []
    for mult in multiplier:
        if mult == 1:
            _spark_df = spark_df
        else:
            _spark_df = spark_df.withColumn("dummy", F.explode(F.lit([1]*mult))).drop("dummy")
            assert N * mult == _spark_df.count(), _spark_df.count()
            pyspark_test_suite(_spark_df)  # force spark to evaluate the duplication
        print(f"{mult=}")
        pyspark_time = timeit.Timer(
            lambda: pyspark_test_suite(_spark_df)).timeit(10) / 10
        print(f"{pyspark_time=}")
        pyspark_times.append(pyspark_time)

    print(f"{pyspark_times=}")

spark.stop()
