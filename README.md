# CS-6220 Programming Assignment 1
## Mining the White House Visitor Log Dataset

### Prerequisites/Libraries used
1. `python3.9+`
1. `numpy`
1. `pandas`
1. `matplotlib`
1. `pyspark`

### Description of the Data
The data comes from the official White House website (https://www.whitehouse.gov). A 12-month time window between June 2022 to May 2023 was chosen. According to the visitor logs, there were 583170 people.

### Code used
`pyspark` is an implementation of `spark` in Python. The code mainly uses the Dataframe/SQL API for finding the required statistics. However, for building histograms, the `RDD` api was used. `matplotlib` was used for building plots.

The main analysis results are shown in `whitehouse-pyspark.ipynb`.

### Timing Experiments
Other hand , we also compare the runtime of the `pyspark` implementation with a `pandas` implementation. `whitehouse-pandas.ipynb` shows the `pandas` implementation. The script is included in `experiment.py`.

#### Methodology
For a comprehensive test, the test suite calculates all of the statistics which were included in `whitehouse-pyspark.ipynb`, minus any graph plotting.

The IO and preprocessing times of the dataframes by each library are not included in the total runtime. This is because in `pandas`, correctly parsing the unconventional datetime formats required a workaround that severely impaired the pre-processing time.

The experiments were repeated with various sizes of a subset of the entire data, and the run times of running each test suite 10 times are reported.

#### Results
![results](results.png)

### Observations
1. On a single machine, Pandas is faster than Pyspark across all sizes of data. This is due to the overhead introduced by RDD fault tolerance and running python code on top of JVM.
1. Pandas is much slower during pre
1. ASDF