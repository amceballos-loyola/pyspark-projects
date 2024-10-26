# Built-in modules
import sys
sys.path.insert(0, ".")

from commons.utils import FolderConfig
from pipeline import Pipeline


if __name__ == "__main__":

    fc = FolderConfig(file=__file__).clean()
    pipeline = Pipeline(
        input_filepath=fc.input / "1342-0.txt",
        output_filepath=fc.output / "simple_count.csv",
        app_name="Counting words in a book",
        master_mode="local[*]"
    ).run_spark()

    pipeline.extract().transform().load()
