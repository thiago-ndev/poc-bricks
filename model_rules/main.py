import argparse

from pyspark.sql import SparkSession

from config_loader import ConfigurationLoader
from job import DataTransformationJob


def create_spark_session(application_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(application_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)

    args = parser.parse_args()

    loader = ConfigurationLoader()
    configuration = loader.load_configuration_from_path(args.config)

    spark = create_spark_session(configuration["model"])

    job = DataTransformationJob(spark, configuration)
    job.execute()

    spark.stop()


if __name__ == "__main__":
    main()