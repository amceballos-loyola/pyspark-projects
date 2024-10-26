import re
import shutil
import logging

from pathlib import Path

from abc import abstractmethod
from pyspark import SparkConf
from pyspark.sql import SparkSession


class Utils:
    COMMA_DELIMITER = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


class FolderConfig:
    def __init__(self, file: str):
        chapter_folder = Path(file).parent.resolve()
        self.input = chapter_folder.parents[1] / "data" / chapter_folder.name
        self.output = chapter_folder.parents[1] / "outputs" / chapter_folder.name
        print(f"Input path: {self.input}")
        print(f"Output path: {self.output}")

    def clean(self):
        logging.warning(f"The folder {self.output} is being cleaned...")
        shutil.rmtree(self.output, ignore_errors=True)
        return self


class IPipeline:
    def __init__(self, app_name: str, master_mode: str):
        self.app_name = app_name
        self.mode = master_mode
        self.spark = None
        self.data = None

    def run_spark(self):
        """Fires up the SparkSession"""
        conf = SparkConf().setAppName(self.app_name).setMaster(self.mode)
        self.spark = (
            SparkSession
            .builder
            .config(conf=conf)
            .getOrCreate()
        )

        return self

    @abstractmethod
    def extract(self):
        """Implements Extracting/Reading operations"""
        return self

    @abstractmethod
    def transform(self):
        """Implements Transformations"""
        return self

    @abstractmethod
    def load(self, n_partitions: int = None):
        """Implements Loading/Writing operations"""
        return self


