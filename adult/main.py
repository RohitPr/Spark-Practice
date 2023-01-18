from ast import alias
from email import header
import json
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import boto3
import csv

spark = SparkSession \
    .builder \
    .appName('spark_app') \
    .master('local[*]').getOrCreate()

s3_client = boto3.client('s3')
s3_bucket_name = 'dept_data'
s3_file_name = 'dept.csv'
s3 = boto3.resource('s3',
                    aws_access_key_id='AWS_ACCESS_KEY_ID',
                    aws_secret_access_key='AWS_SECRET_ACCESS_KEY')

with open('dept_file', 'wb') as data:
    s3.download_fileobj(s3_bucket_name, s3_file_name, data)

spark_df = spark.read.csv('./dept.csv')

dept_prod_df = spark_df.groupBy('dept').count()

dept_prod_list = dept_prod_df.select(
    'dept').rdd.flatMap(lambda x: x).collect()

avg_dept_df = spark_df.groupBy('dept').agg(
    avg('emp_salary'), alias='avg_salary')

for dept in dept_prod_list:
    filter_dept_df = avg_dept_df.filter(avg_dept_df.dept == f'{dept}')
    filter_dept_df.coalesce(1).write.csv(f'{dept}_avg_data', header=True)
