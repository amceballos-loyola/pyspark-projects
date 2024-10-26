import sys
sys.path.insert(0, "../04")

from commons.utils import FolderConfig
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf = SparkConf().setAppName("Adding up prime numbers").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(str(fc.input / "prime_nums.text"))
    numbers = lines.flatMap(lambda line: line.split("\t"))

    validNumbers = numbers.filter(lambda number: number)
    intNumbers = validNumbers.map(lambda number: int(number))

    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))