import json
import logging

from pyspark.sql.types import *

from common import process_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] - %(message)s")


def read_json_data(config):
    json_file_path = config["local"]["json_file_path"]

    with open(json_file_path, "r") as f:
        json_data_list = json.load(f)

    return json_data_list


if __name__ == "__main__":
    process_data(read_json_data)
