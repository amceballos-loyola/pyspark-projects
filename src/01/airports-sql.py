# Built-in modules
import sys
sys.path.insert(0, ".")

# Third-party modules
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# User-defined modules
from commons.utils import FolderConfig


airports_schema = StructType(
    [
        StructField("Id", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("IATA_code", StringType(), True),
        StructField("ICAO_code", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Altitude", DoubleType(), True),
        StructField("Timezone_DST", StringType(), True),
        StructField("Timezone_Oslon", StringType(), True),
    ]
)

if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf_args = SparkConf().setAppName("USA Airports").setMaster("local[*]")
    spark = (
        SparkSession
        .builder
        .config(conf=conf_args)
        .getOrCreate()
    )

    airports = (
        spark.read.csv(str(
            fc.input / "airports.text"),
            header=False,
            schema=airports_schema)
    )
    usa_airports = (
        airports
        .select("Name", "City")
        .where(col("Country") == "United States")
    )

    usa_airports.write.json(str(fc.output / "airports_in_usa"))