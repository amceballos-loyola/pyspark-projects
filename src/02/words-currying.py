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

    conf_args = SparkConf().setAppName("Counting words in a book - Cascading").setMaster("local[*]")
    spark = (
        SparkSession
        .builder
        .config(conf=conf_args)
        .getOrCreate()
    )

    # PUTTING ALL TOGETHER
    results = (
        spark.read.text(str(fc.input / "1342-0.txt"))
        .select(F.split(F.col("value"), " ").alias("line"))
        .select(F.explode(F.col("line")).alias("word"))
        .select(F.lower(F.col("word")).alias("word"))
        .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
        .where(F.col("word") != "")
        .groupby(F.col("word"))
        .count()
    )

    results.orderBy("count", ascending=False).show(10)
    results.write.csv(str(fc.output / "simple_count.csv"))
    #results.coalesce(1).write.csv(str(fc.output / "simple_count.csv"))

