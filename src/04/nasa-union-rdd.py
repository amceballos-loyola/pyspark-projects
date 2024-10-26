import sys
sys.path.insert(0, "../04")

from commons.utils import FolderConfig
from pyspark import SparkContext, SparkConf

def isNotHeader(line: str):
    return not (line.startswith("host") and "bytes" in line)


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf = SparkConf().setAppName("NASALogs-Union").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    julyFirstLogs = sc.textFile(str(fc.input / "nasa_19950701.tsv"))
    augustFirstLogs = sc.textFile(str(fc.input / "nasa_19950801.tsv"))

    aggregatedLogLines = julyFirstLogs.union(augustFirstLogs)

    cleanLogLines = aggregatedLogLines.filter(isNotHeader)
    sample = cleanLogLines.sample(withReplacement=True, fraction=0.1)

    sample.saveAsTextFile(str(fc.output / "sample_nasa_logs.csv"))