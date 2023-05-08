import json
from typing import Callable

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def init_spark(app_name, warehouse_dir):
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)

    spark = (
        SparkSession.builder
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


def infer_schema(spark, json_data_list):
    # Create a temporary DataFrame to infer the schema
    temp_df = spark.read.json(spark.sparkContext.parallelize(
        [json.dumps(json_data_list[0])]), multiLine=True)

    # Return the inferred schema
    return temp_df.schema


def create_hive_table(spark, data, database_name, table_name):
    # Create the Hive database if needed
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    hive_table = f"{database_name}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {hive_table}")

    data.createOrReplaceTempView("data_view")
    spark.sql(
        f"CREATE TABLE {hive_table} USING parquet OPTIONS('compression'='snappy') AS SELECT * FROM data_view")


def read_config(config_file):
    with open(config_file, "r") as f:
        config = json.load(f)
    return config


def process_data(read_json_data: Callable):
    # Read the config.json file
    config = read_config("config.json")

    # Initialize Spark
    spark = init_spark("Spark-Streaming", config["local"]["warehouse_dir"])

    # Get the JSON data
    json_data_list = read_json_data(config)

    # Get schema from the JSON data
    schema = infer_schema(spark, json_data_list)

    # Create a DataFrame from the data
    data = spark.createDataFrame(json_data_list, schema=schema)

    # Load the Hive table
    create_hive_table(
        spark, data, config["database_name"], config["table_name"])


if __name__ == "__main__":
    raise Exception(
        "Please run either google_cloud_script.py or local_script.py")
