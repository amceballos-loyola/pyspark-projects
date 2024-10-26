# Third-party modules
from pyspark.sql.functions import (
    col, split, explode, lower, regexp_extract
)

# User-defined modules
from commons.utils import IPipeline


class Pipeline(IPipeline):
    def __init__(self, input_filepath: str, output_filepath: str, app_name: str, master_mode: str):
        super().__init__(app_name, master_mode)

        self.input_file = input_filepath
        self.output_file = output_filepath

    def extract(self):
        self.data = self.spark.read.text(str(self.input_file))
        return self

    def _tokenize(self):
        self.data = (
            self.data
            .select(split(self.data.value, " ").alias("line"))
            .select(explode(col("line")).alias("word"))
            .select(lower(col("word")).alias("word_lower"))
        )
        return self

    def _clean(self):
        self.data = (
            self.data
            .select(regexp_extract(col("word_lower"), "[a-z]*", 0).alias("word"))
            .where(col("word") != "")
        )
        return self

    def _count(self):
        self.data = self.data.groupby(col("word")).count()
        return self

    def transform(self):
        return self._tokenize()._clean()._count()

    def load(self, n_partitions: int = None):
        if n_partitions and n_partitions > 0:
            self.data = self.data.coalesce(n_partitions)

        self.data.write.csv(str(self.output_file))
