import sys
sys.path.insert(0, ".")

import logging

from commons.utils import FolderConfig
from pipeline import Pipeline, InputPaths

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    fc = FolderConfig(file=__file__).clean()

    pipeline = Pipeline(
        input_paths=InputPaths(
            broadcast_logs=fc.input / "broadcast_logs_2018_q3_m8_sample.csv",
            identifier_logs=fc.input / "ReferenceTables" / "LogIdentifier.csv",
            categories=fc.input / "ReferenceTables" / "CD_Category.csv",
            program_class=fc.input / "ReferenceTables" / "CD_ProgramClass.csv",
        ),
        output_path=fc.output / "simple_count.csv",
        app_name="Counting commercials",
        master_mode="local[*]"
    ).run_spark()

    pipeline.extract().transform().load()
    pipeline.result.orderBy("commercial_ratio", ascending=False).show(10, False)
