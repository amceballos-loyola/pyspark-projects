# Built-in modules
import sys
sys.path.insert(0, ".")

# Third-party modules
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, lower, regexp_extract
)

# User-defined modules
from commons.utils import FolderConfig


if __name__ == "__main__":

    fc = FolderConfig(file=__file__).clean()

    conf_args = SparkConf().setAppName("Counting words in a book").setMaster("local[*]")
    spark = (
        SparkSession
        .builder
        .config(conf=conf_args)
        .getOrCreate()
    )

    # 1 - READ
    book = spark.read.text(str(fc.input / "1342-0.txt"))

    # 2 - TOKEN (TRANSFORM)
    lines = book.select(split(book.value, " ").alias("line"))
    words = lines.select(explode(col("line")).alias("word"))
    words_lower = words.select(lower(col("word")).alias("word_lower"))

    # 3 - CLEAN
    words_clean = words_lower.select(regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word"))
    words_nonull = words_clean.where(col("word") != "")

    # 4 - COUNT
    results = words_nonull.groupby(col("word")).count()

    # 5 - ANSWER
    results.orderBy(col("count").desc()).show(10)

    # 6 - WRITE
    results.write.csv(str(fc.output / "simple_count.csv"))
    #results.coalesce(1).write.csv(str(fc.output / "simple_count.csv"))
