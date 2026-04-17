import json
from pathlib import Path
from urllib.parse import urlparse
import boto3

class ConfigurationLoader:
    def load_configuration_from_path(self, configuration_path: str) -> dict:
        if configuration_path.startswith("s3://"):
            return self._load_configuration_from_s3(configuration_path)

        return self._load_configuration_from_local_file(configuration_path)

    @staticmethod
    def _load_configuration_from_local_file(self, file_path: str) -> dict:
        with Path(file_path).open("r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def _load_configuration_from_s3(self, s3_path: str) -> dict:
        parsed_path = urlparse(s3_path)
        bucket_name = parsed_path.netloc
        object_key = parsed_path.path.lstrip("/")

        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        file_content = response["Body"].read().decode("utf-8")
        return json.loads(file_content)
