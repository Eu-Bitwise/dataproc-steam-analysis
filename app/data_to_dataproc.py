import json
import logging

from google.cloud import storage
from pyspark.sql.types import *

from common import process_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] - %(message)s")


def read_json_data(config):
    bucket_name = config["google_cloud"]["bucket_name"]
    prefix = config["google_cloud"]["prefix"]

    # Retrieve the object from GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.blob.Blob(prefix, bucket)
    json_data_str = blob.download_as_text()

    json_data_list = json.loads(json_data_str)

    return json_data_list


if __name__ == "__main__":
    process_data(read_json_data)

