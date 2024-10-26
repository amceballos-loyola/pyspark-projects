import sys
sys.path.insert(0, "../01")

from commons.utils import Utils, FolderConfig
from pyspark import SparkContext, SparkConf


def split_comma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[2])


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf = SparkConf().setAppName("airports").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    airports = sc.textFile(str(fc.input / "airports.text"))
    usa_airports = (
        airports
        .filter(lambda line:
                Utils.COMMA_DELIMITER.split(line)[3] == '"United States"')
    )

    result = usa_airports.map(split_comma)
    result.saveAsTextFile(str(fc.output / "airports_in_usa.text"))