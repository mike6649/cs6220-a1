import os
import numpy as np
import pandas as pd
from functools import reduce

import timeit
from datetime import datetime

os.system("mkdir data")
os.system("wget -c -q -i links.txt -P data")

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

    timestamp_cols = ["Appointment Made Date", "Appointment Start Date",
                      "Appointment End Date", "Appointment Cancel Date", "Last Entry Date"]
    for ts_col in timestamp_cols:
        df[ts_col] = df[~df[ts_col].isna()][ts_col].apply(lambda x: pd.to_datetime(
            datetime.strptime(x, "%m/%d/%Y %H:%M"), infer_datetime_format=True) if x is not None else None)

    def parseTOA(x):
        if x is None:
            return None
        try:
            return pd.to_datetime(datetime.strptime(x, "%b %d %Y %H:%M%p"))
        except:
            return None
    df.TOA = df[~df.TOA.isna()].TOA.apply(parseTOA)
    return df


def pandas_test_suite(df):
    # 10 most frequent visitors
    df.groupby(["First Name", "Last Name", "Middle Initial"]
               ).size().sort_values(ascending=False).head(10)
    # The 10 most frequently visited people (visitee_namelast, visitee_namefirst) in the White House.
    df.groupby(["Visitee First Name", "Visitee Last Name"]
               ).size().sort_values(ascending=False).head(10)
    # The 10 most frequent visitor-visitee combinations.
    df.groupby(["First Name", "Middle Initial", "Last Name", "Visitee First Name",
               "Visitee Last Name"]).size().sort_values(ascending=False).head(10)

    # lateness histogram
    delta = (df[~df.TOA.isna()].TOA - df[~df.TOA.isna()]
             ["Appointment Start Date"]).dt.seconds / 60
    bins = pd.cut(delta, list(np.linspace(-80, +120, 50)))
    bins[~bins.isna()].groupby(bins).count()

    # duration histogram
    duration = (df["Appointment End Date"] -
                df["Appointment Start Date"]).dt.seconds / 60
    bins = pd.cut(duration, list(np.linspace(0, +1200, 100)))
    bins[~bins.isna()].groupby(bins).count()

    # people histogram
    people = df["Total People"]
    bins = pd.cut(people, list(np.linspace(0, 200, 50)))
    bins[~bins.isna()].groupby(bins).count()

print("Loading pandas dataframe")
pd_df = create_pandas_df()

if __name__ == "__main__":
    print("started scripts")
    multiplier = [1, 2, 5, 10, 20]
    N = len(pd_df)
    pandas_times = []
    for mult in multiplier:
        if mult == 1:
            _pd_df = pd_df
        else:
            _pd_df = pd.DataFrame(np.repeat(pd_df.values, mult, axis=0))
            _pd_df.columns = pd_df.columns
            assert N * mult == len(_pd_df), len(_pd_df)
        print(f"{mult=}")
        pandas_time = timeit.Timer(
            lambda: pandas_test_suite(_pd_df)).timeit(10) / 10
        print(f"{pandas_time=}")
        pandas_times.append(pandas_time)

    print(f"{pandas_times=}")

