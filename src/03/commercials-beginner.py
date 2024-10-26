import os
import sys
sys.path.insert(0, ".")

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from commons.utils import FolderConfig


if __name__ == "__main__":
    fc = FolderConfig(file=__file__).clean()

    spark = SparkSession.builder.appName(
        "Getting the Canadian TV channels with the highest/lowest proportion of commercials."
    ).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1 - Reading all the relevant data sources (pipeline.extract())

    DIRECTORY = str(fc.input)

    logs = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "broadcast_logs_2018_q3_m8_sample.csv"),
            sep="|",
            header=True,
            inferSchema=True,
            timestampFormat="yyyy-MM-dd")
    )

    log_identifier = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "ReferenceTables/LogIdentifier.csv"),
            sep="|",
            header=True,
            inferSchema=True)
    )

    cd_category = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "ReferenceTables/CD_Category.csv"),
            sep="|",
            header=True,
            inferSchema=True)
        .select(
            "CategoryID",
            "CategoryCD",
            F.col("EnglishDescription").alias("Category_Description"))
    )

    cd_program_class = (
        spark.read.csv(
            path=os.path.join(DIRECTORY, "ReferenceTables/CD_ProgramClass.csv"),
            sep="|",
            header=True,
            inferSchema=True)
        .select(
            "ProgramClassID",
            "ProgramClassCD",
            F.col("EnglishDescription").alias("ProgramClass_Description"))
    )

    # 2 - Data processing

    logs = logs.drop("BroadcastLogID", "SequenceNO")
    logs = (
        logs.withColumn(
            colName="duration_seconds",
            col=(  F.col("Duration").substr(1, 2).cast("int") * 60 * 60
                 + F.col("Duration").substr(4, 2).cast("int") * 60
                 + F.col("Duration").substr(7, 2).cast("int") )
        )
    )

    logs\
        .select("LogServiceID", "LogDate", "ProgramTitle", "Duration", "duration_seconds")\
        .orderBy("duration_seconds", ascending=False).show(10)

    # input("Press [ENTER] to continue...")

    log_identifier = log_identifier.where(F.col("PrimaryFG") == 1)

    logs_and_channels = \
        logs.join(
            other=log_identifier,
            on="LogServiceID"
        )

    full_log = \
        logs_and_channels.join(
            other=cd_category,
            on="CategoryID",
            how="left"
        ).join(
            other=cd_program_class,
            on="ProgramClassID",
            how="left"
        )

    full_log\
        .select("LogIdentifierID", "duration_seconds")\
        .orderBy("duration_seconds", ascending=False)\
        .show(10)

    #input("Press [ENTER] to continue...")

    # Understanding the second part of the following query.
    # Total duration of a program class
    (full_log
        .groupby("ProgramClassCD", "ProgramClass_Description")
        .agg(F.sum("duration_seconds").alias("duration_total"))
        .orderBy("duration_total", ascending=False).show(10, False))

    # 3 - Answer to what are the channels that spent more time in commercials
    answer = (
        full_log
        .groupby("LogIdentifierID")
        .agg(
            F.sum(
                F.when(
                    condition=F.trim(F.col("ProgramClassCD")).isin(
                        ["COM", "PRC", "PGI", "PRO", "LOC", "SPO", "MER", "SOL"]),
                    value=F.col("duration_seconds"),
                ).otherwise(0)
            ).alias("duration_commercial"),
            F.sum("duration_seconds").alias("duration_total"))
        .withColumn("commercial_ratio", F.col("duration_commercial") / F.col("duration_total"))
        .fillna(0) # |<-- fill null values with 0. Other option to check it out is .dropna()
    )

    answer.orderBy("commercial_ratio", ascending=False).show(10, False)
