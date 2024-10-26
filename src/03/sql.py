#region Modules
#region Sys modules
import sys
sys.path.insert(0, ".")
#endregion

#region User defined modules
from commons.utils import FolderConfig
#endregion

#region Spark modules
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
#endregion
#endregion

if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf_args = SparkConf().setAppName("Canadian Radio-Television and Telecommunications Commission").setMaster("local[*]")
    spark = (
        SparkSession
        .builder
        .config(conf=conf_args)
        .getOrCreate()
    )

    # 1 - READ
    logs = spark.read.csv(
        path=str(fc.input / "broadcast_logs_2018_q3_m8_sample.csv"),
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd"
    )

    logs.printSchema()

    # 2 - Peeking the data frame in chunks of three columns
    import numpy as np
    column_split = np.array_split(np.array(logs.columns), len(logs.columns))
    print(column_split)

    # 3 - Getting rid of the columns
    logs = logs.drop("BroadcastLogID", "SequenceNO")
    print("BroadcastLogID" in logs.columns)     # => False
    print("SequenceNo" in logs.columns)         # => False

    # 4 - Selecting and displaying the Duration column
    dt = logs.select(F.col("Duration"))
    dt.show(5)
    print(dt.dtypes)

    # 5 - Extracting data from a string column
    logs.select(
        F.col("Duration"),
        F.col("Duration").substr(1,2).cast("int").alias("hours"),
        F.col("Duration").substr(4,2).cast("int").alias("minutes"),
        F.col("Duration").substr(7,2).cast("int").alias("seconds")
    ).distinct().show(5)

    # 6 - Adding a new column with the duration in seconds
    # changing select by withColumn we create a new column at the end of our data frame
    logs.select(
        F.col("Duration"),
        (
            F.col("Duration").substr(1, 2).cast("int") * 60 * 60
            + F.col("Duration").substr(4, 2).cast("int") * 60
            + F.col("Duration").substr(7, 2).cast("int")
        ).alias("Duration_seconds"),
    ).distinct().show(5)

    # 7 - Renaming one column
    logs = logs.withColumnRenamed("Duration_seconds", "duration_seconds")
    logs.printSchema()

    logs.toDF(*[x.lower() for x in logs.columns]).printSchema()

    # 8 - Describing everything in one fell swoop. A quick look at the data during development
    for c in logs.columns:
        logs.describe(c).show()
        #logs.select(c).summary("min", "10%", "90%", "max").show()