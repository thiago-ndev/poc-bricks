from pyspark.sql import SparkSession

from rule_engine import DataTransformationEngine
from io_manager import DataIOManager


class DataTransformationJob:
    def __init__(self, spark_session: SparkSession, configuration: dict):
        self.spark_session = spark_session
        self.configuration = configuration
        self.engine = DataTransformationEngine(configuration)
        self.io_manager = DataIOManager()

    def execute(self):
        input_dataframe = self.io_manager.read_input_dataframe(
            self.spark_session,
            self.configuration["source"]
        )

        transformed_dataframe, ordered_columns = self.engine.apply_all_rules_to_dataframe(
            input_dataframe
        )

        final_dataframe = transformed_dataframe.select(*ordered_columns)

        self.io_manager.write_output_dataframe(
            final_dataframe,
            self.configuration["target"]
        )