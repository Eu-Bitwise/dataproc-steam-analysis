# JSON streaming for Spark or Dataproc

This project demonstrates how to stream JSON data for: Google Cloud Dataproc or a local Spark instance.
The script reads the JSON data, infers the schema, and then creates/loads a Hive table with the data for quick insight and integration.

## Requirements

- Python 3.6 or higher
- [pyspark](https://pypi.org/project/pyspark/)
- [google-cloud-storage](https://pypi.org/project/google-cloud-storage/) (only required for the data_to_dataproc.py script)

To install the required packages, you can run:

`pip install pyspark google-cloud-storage`

## Configuration
Update the `config.json` file with the appropriate settings for your use case.
This file contains separate configurations for Google Cloud Storage and local Spark instances, as well as the database and table names.

## Usage
Run the appropriate script based on your desired instance:

### For Google Cloud Dataproc

submit a new job for your cluster: 
`gcloud dataproc jobs submit pyspark app/data_to_dataproc.py --cluster=<CLUSTER-NAME> --region=<REGION> --py-files= app/common.py --files=config.json`

### For a local Spark instance
simply run: `python app/data_to_spark.py`:
