from email import header
import json
from threading import local
import datetime
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import inspect
import os.path

spark = SparkSession \
    .builder \
    .appName('Spark') \
    .master('local[*]') \
    .config('spark.sql.analyzer.failAmbiguousSelfJoin', False).getOrCreate()


read_df = spark.read.csv(
    './bank_nifty_data/banknifty.csv', header=True, inferSchema=True)

read_df.show()

