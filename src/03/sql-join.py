# Built-in modules
import sys
sys.path.insert(0, ".")

# Third-party modules
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# User-defined modules
from commons.utils import FolderConfig


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf_args = \
        SparkConf()\
        .setAppName("Canadian Radio-Television and Telecommunications Commission")\
        .setMaster("local[*]")

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

    log_identifier = spark.read.csv(
        path=str(fc.input / "ReferenceTables/LogIdentifier.csv"),
        sep="|",
        header=True,
        inferSchema=True,
        timestampFormat="yyyy-MM-dd"
    )

    log_identifier.printSchema()

    # 2 - Exploring our first link table
    log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)
    print(log_identifier).show(5)

    # 3 - Join pattern -- A bare-bone recipe for a join in PySpark
    # [LEFT].join(
    #   [RIGHT],
    #   on=[PREDICATES]
    #   how=[METHOD] (inner, left
    # )
    logs.join(                  # logs dataframe is the left table (LT)
        other=log_identifier,   # log_identifier is the right table (RT)
        on="LogServiceID",      # what records in the LF match the RT. It all about rules of LDF and RDT
        how="inner"             # it should be a match between LT and RT
    )

