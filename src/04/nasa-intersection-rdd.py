import sys
sys.path.insert(0, "../04")

from commons.utils import FolderConfig
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    conf = SparkConf().setAppName("NASALogs-SameHosts").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    julyFirstLogs = sc.textFile(str(fc.input / "nasa_19950701.tsv"))
    augustFirstLogs = sc.textFile(str(fc.input / "nasa_19950801.tsv"))

    julyFirstHosts = julyFirstLogs.map(lambda line: line.split("\t")[0])
    augustFirstHosts = augustFirstLogs.map(lambda line: line.split("\t")[0])

    intersection = julyFirstHosts.intersection(augustFirstHosts)

    cleanedHostIntersection = intersection.filter(lambda host: host != "host")
    cleanedHostIntersection.saveAsTextFile(str(fc.output / "nasa_logs_same_hosts.csv"))