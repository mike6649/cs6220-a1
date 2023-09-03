import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import timeit
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("whitehouse").getOrCreate()


# we ignore the time of loading the CSVs and data pre-processing

def create_pandas_df():
    import glob

    all_files = glob.glob("data/*.csv")
    _dtypes1 = {
        'Last Name': 'string',
        'First Name': 'string',
        'Middle Initial': 'string',
        'UIN': 'string',
        'BDGNBR': 'float64',
        'Access Type': 'string',
        'TOA': 'string',
        'POA': 'string',
        'TOD': 'string',
        'POD': 'string',
        'Appointment Made Date': 'string',
        'Appointment Start Date': 'string',
        'Appointment End Date': 'string',
        'Appointment Cancel Date': 'string',
        'Total People': 'int64',
        'Last Updated By': 'string',
        'POST': 'string',
        'Last Entry Date': 'string',
        'Terminal Suffix': 'string',
        'Visitee Last Name': 'string',
        'Visitee First Name': 'string',
        'Meeting Location': 'string',
        'Meeting Room': 'string',
        'Caller Last Name': 'string',
        'Caller First Name': 'string',
        'CALLER_ROOM': 'string',
        'RELEASEDATE': 'string',
        'Caller Room': 'string',
        'Release Date': 'string',
    }


    li = [pd.read_csv(filename, dtype=_dtypes1) for filename in all_files]
    for _df in li:
        _df.columns = li[0].columns
    df = pd.concat(li, axis=0, ignore_index=True)

    timestamp_cols = ["Appointment Made Date", "Appointment Start Date", "Appointment End Date", "Appointment Cancel Date", "Last Entry Date"]
    for ts_col in timestamp_cols:
        df[ts_col] = df[~df[ts_col].isna()][ts_col].apply(lambda x: pd.to_datetime(datetime.strptime(x, "%m/%d/%Y %H:%M"), infer_datetime_format=True) if x is not None else None)

    def parseTOA(x):
        if x is None:
            return None
        try:
            return pd.to_datetime(datetime.strptime(x, "%b %d %Y %H:%M%p"))
        except:
            return None
    df.TOA = df[~df.TOA.isna()].TOA.apply(parseTOA)
    return df


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


def pandas_test_suite(df):
    # 10 most frequent visitors
    df.groupby(["First Name", "Last Name", "Middle Initial"]).size().sort_values(ascending=False).head(10)
    # The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    df.groupby(["Visitee First Name", "Visitee Last Name"]).size().sort_values(ascending=False).head(10)
    # The 10 most frequent visitor-visitee combinations.
    df.groupby(["First Name", "Middle Initial", "Last Name", "Visitee First Name", "Visitee Last Name"]).size().sort_values(ascending=False).head(10)

    # lateness histogram
    delta = (df[~df.TOA.isna()].TOA - df[~df.TOA.isna()]["Appointment Start Date"]).dt.seconds / 60
    bins = pd.cut(delta, list(np.linspace(-80, +120, 50)))
    bins[~bins.isna()].groupby(bins).count()

    # duration histogram
    duration = (df["Appointment End Date"] - df["Appointment Start Date"]).dt.seconds / 60
    bins = pd.cut(duration, list(np.linspace(0, +1200, 100)))
    bins[~bins.isna()].groupby(bins).count()

    # people histogram
    people = df["Total People"]
    bins = pd.cut(people, list(np.linspace(0, 200, 50)))
    bins[~bins.isna()].groupby(bins).count()


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
print("Loading pandas dataframe")
pd_df = create_pandas_df()

if __name__ == "__main__":
    print("started scripts")
    subset_fracs = [.1, .2, .5, 1]
    pandas_times = []
    pyspark_times = []
    for frac in subset_fracs:
        if frac == 1:
            _spark_df = spark_df
            _pd_df = pd_df
        else:
            _spark_df = spark_df.sample(withReplacement=False, fraction=frac)
            _pd_df = pd_df.sample(replace=False, frac=frac)
            pyspark_test_suite(_spark_df) # force spark to sample the data
        print(f"{frac=}")
        pandas_time = timeit.Timer(lambda: pandas_test_suite(_pd_df)).timeit(10)
        pyspark_time = timeit.Timer(lambda: pyspark_test_suite(_spark_df)).timeit(10)
        print(f"{pandas_time=}, {pyspark_time=}")
        pandas_times.append(pandas_time)
        pyspark_times.append(pyspark_time)

    print(f"{pandas_times=}")
    print(f"{pyspark_times=}")

spark.stop()
