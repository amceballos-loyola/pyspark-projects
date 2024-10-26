import dataclasses
import logging
import os
import pyspark.sql.functions as psf

from enum import Enum
from commons.utils import IPipeline


class TbName(str, Enum):
    broadcast_logs = "broadcast_logs"
    identifier_logs = "identifier_logs"
    categories = "categories"
    program_class = "program_class"
    result = "result"


@dataclasses.dataclass
class InputPaths:
    broadcast_logs: os.PathLike
    identifier_logs: os.PathLike
    categories: os.PathLike
    program_class: os.PathLike


class Pipeline(IPipeline):
    def __init__(self, input_paths: InputPaths, output_path: os.PathLike, app_name: str, master_mode: str):
        super().__init__(app_name, master_mode)

        self.input_paths = input_paths
        self.output_path = output_path
        self.data = {}

    @property
    def result(self):
        return self.data["result"]

    def extract(self) -> "Pipeline":
        for (tb_name, tb_path) in dataclasses.asdict(self.input_paths).items():
            logging.info(f"Reading table: {tb_name}")
            self.data[tb_name] = self.spark.read.csv(
                path=str(tb_path),
                sep="|",
                header=True,
                inferSchema=True,
                timestampFormat="yyyy-MM-dd"
            )

        self.data[TbName.categories] = self.data[TbName.categories].select(
            "CategoryID",
            "CategoryCD",
            psf.col("EnglishDescription").alias("Category_Description")
        )

        self.data[TbName.program_class] = self.data[TbName.program_class].select(
            "ProgramClassID",
            "ProgramClassCD",
            psf.col("EnglishDescription").alias("ProgramClass_Description")
        )

        return self

    def _clean_broadcast_log(self) -> "Pipeline":
        logging.info("Cleaning broadcast logs")
        self.data[TbName.broadcast_logs] = (
            self.data[TbName.broadcast_logs]
            .drop("BroadcastLogID", "SequenceNO")
            .withColumn(
                colName="duration_seconds",
                col=(psf.col("Duration").substr(1, 2).cast("int") * 60 * 60
                     + psf.col("Duration").substr(4, 2).cast("int") * 60
                     + psf.col("Duration").substr(7, 2).cast("int"))
            )
        )
        return self

    def _clean_identifier_logs(self) -> "Pipeline":
        logging.info("Cleaning identifier logs")
        self.data[TbName.identifier_logs] = self.data[TbName.identifier_logs].where(psf.col("PrimaryFG") == 1)
        return self

    def _merge(self) -> "Pipeline":
        logging.info("Merging tables")
        self.data[TbName.result] = (
            self.data[TbName.broadcast_logs]
            .join(
                other=self.data[TbName.identifier_logs],
                on="LogServiceID"
            )
            .join(
                other=self.data[TbName.categories],
                on="CategoryID",
                how="left"
            )
            .join(
                other=self.data[TbName.program_class],
                on="ProgramClassID",
                how="left"
            )
        )
        return self

    def _compute_commercial_ratio(self) -> "Pipeline":
        logging.info("Computing commercial ratio")
        self.data[TbName.result] = (
            self.data[TbName.result].groupby("LogIdentifierID")
            .agg(
                psf.sum(
                    psf.when(
                        condition=psf.trim(psf.col("ProgramClassCD")).isin(
                            ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]
                        ),
                        value=psf.col("duration_seconds"),
                    ).otherwise(0)).alias("duration_commercial"),
                psf.sum("duration_seconds").alias("duration_total"))
            .withColumn("commercial_ratio", psf.col("duration_commercial") / psf.col("duration_total"))
            .fillna(0)  # |<-- fill null values with 0. Other option to check it out is .dropna()
        )
        return self

    def transform(self) -> "Pipeline":
        logging.info("Running transform pipeline:")
        return (
            self
            ._clean_broadcast_log()
            ._clean_identifier_logs()
            ._merge()
            ._compute_commercial_ratio()
        )

    def load(self, n_partitions: int = None) -> "Pipeline":
        logging.info("Saving data...")
        if n_partitions and n_partitions > 0:
            self.data[TbName.result] = self.data[TbName.result].coalesce(n_partitions)

        self.data[TbName.result].write.csv(str(self.output_path))
        return self
