# Built-in modules
import sys
import os
# Third-party modules
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = r"C:\Users\Alvaro\Local folder\Local Loyola\aplicaciones-i\.venv\Scripts\python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = r"C:\Users\Alvaro\Local folder\Local Loyola\aplicaciones-i\.venv\Scripts\python.exe"

# How to use this file:
# spark-submit src/00/mnm-count.py data/00/mnm_dataset.csv
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnm-count <file>", file=sys.stderr)
        sys.exit(-1)

    # THE BIG QUESTION:
    # What's the students favorite color of M&M in each state
    # and particularly in California

    # 1 - Instantiate the spark session
    spark = (
        SparkSession
        .builder
        .appName("MnM Count")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    # LogLevels: OFF < FATAL < ERROR < WARN < INFO < DEBUG < TRACE < ALL

    # 2 - Read data
    # 2.1 Get the M&M data set file name from the command line
    mnm_file = sys.argv[1]

    # 2.2 Read the file into a Spark DataFrame
    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    mnm_df.show(n=5, truncate=False)

    # 3 - Aggregate count of all colors and groupBy state and color
    #     orderBy descending order
    count_mnm_df = (
        mnm_df
        .select("State", "Color", "Count")          # transformation
        .groupBy("State", "Color")                  # transformation
        .sum("Count")                               # action (aggregate function)
        .orderBy("sum(Count)", ascending=False)     # transformation
    )

    # 4 - Show all the resulting aggregation
    count_mnm_df.show(n=60, truncate=False)         # action
    print(f"Total Rows = {count_mnm_df.count()}")

    # 5 - Find the aggregate count for California by filtering
    ca_count_mnm_df = (
        mnm_df.select("*")                          # narrow transformation (1-to-1)
        .where(mnm_df.State == "CA")                # narrow transformation (1-to-1)
        .groupBy("State", "Color")                  # wide transformation   (1-to-many)
        .sum("Count")                               # action (aggregate function)
        .orderBy("sum(Count)", ascending=False)     # wide transformation   (1-to-many)
    )

    # 6 - ANSWER: showing the result of aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
