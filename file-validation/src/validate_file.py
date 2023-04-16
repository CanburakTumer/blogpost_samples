#!/usr/bin/env python

import sys
import os
import json

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from google.cloud import storage

if len(sys.argv) != 4:
  raise Exception("Exactly 3 arguments are required: `<source> <data_file> <schema_file>")

###
# Define helper methods
###

def convert_bqschema_to_structtype(bq_schema):
    '''
    ' Converts exported BQ Schema to Spark StructType schema
    ' Schema has been exported from BigQuery by following command
    ' `bq show --format prettyjson  <project>:<dataset>.<table> | jq '.schema.fields'`
    '''
    new_schema = StructType()

    for row in bq_schema:
        if row['type'] == 'INTEGER':
            new_schema.add(row['name'], IntegerType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'STRING':
            new_schema.add(row['name'], StringType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'DATETIME':
            new_schema.add(row['name'], TimestampType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'TIME':
            new_schema.add(row['name'], TimestampType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'DATE':
            new_schema.add(row['name'], DateType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'TIMESTAMP':
            new_schema.add(row['name'], TimestampType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'BOOLEAN':
            new_schema.add(row['name'], BooleanType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'BIGNUMERIC':
            new_schema.add(row['name'], LongType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'NUMERIC':
            new_schema.add(row['name'], DecimalType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'FLOAT':
            new_schema.add(row['name'], FloatType(), True if row['mode'] == 'NULLABLE' else False)
        elif row['type'] == 'BYTES':
            new_schema.add(row['name'], ByteType(), True if row['mode'] == 'NULLABLE' else False)

    return new_schema

###
# Initiate variables
###
processing_environment = sys.argv[1] # must be local or gcp

data_file = sys.argv[2]
data_file_extension = data_file.split('.')[-1]
data_file_format = data_file_extension

schema_file = sys.argv[3]
if processing_environment == 'gcp':
    schema_file_bucket_id = schema_file.split('/')[2]
    schema_file_path = '/'.join(schema_file.split('/')[3:])

###
# Read schema from BQ Schema and convert
###
if processing_environment == 'gcp':
    client = storage.Client()
    bucket = client.get_bucket(schema_file_bucket_id)
    schema_file_handle = bucket.get_blob(schema_file_path).download_to_filename('/schema_local_copy.txt')

    with open('/schema_local_copy.txt','r') as schema:
        bq_schema = json.loads(schema.read())
elif processing_environment == 'local':
    with open(schema_file,'r') as schema:
        bq_schema = json.loads(schema.read())

converted_schema = convert_bqschema_to_structtype(bq_schema)

###
# Validate data file schema via Spark
###
os.environ["PYSPARK_SUBMIT_ARGS"]='--packages org.apache.spark:spark-avro_2.12:3.3.0 pyspark-shell'
sc = pyspark.SparkContext()
ss = SparkSession(sc)
try:
    df = ss.read.format(data_file_format).schema(converted_schema).option("header","true").option("mode","FAILFAST").load(data_file)
    df.printSchema()
    df.show()
except Exception as e:
    print("Schema could not be validated")
    exit(1)