from pyspark.sql import SparkSession, DataFrame


class DataIOManager:
    def read_input_dataframe(self, spark_session: SparkSession, source_config: dict) -> DataFrame:
        data_format = source_config["format"]
        data_path = source_config["path"]

        dataframe_reader = spark_session.read.format(data_format)

        for option_key, option_value in source_config.get("read_options", {}).items():
            dataframe_reader = dataframe_reader.option(option_key, option_value)

        return dataframe_reader.load(data_path)

    def write_output_dataframe(self, dataframe: DataFrame, target_config: dict):
        data_format = target_config["format"]
        output_path = target_config["path"]
        write_mode = target_config.get("mode", "overwrite")

        dataframe_writer = dataframe.write.mode(write_mode).format(data_format)

        if "compression" in target_config:
            dataframe_writer = dataframe_writer.option(
                "compression",
                target_config["compression"]
            )

        dataframe_writer.save(output_path)