from email import header
from threading import local
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession \
    .builder \
    .appName('Spark') \
    .master('local[*]') \
    .config('spark.sql.analyzer.failAmbiguousSelfJoin', False).getOrCreate()


read_df = spark.read.csv(
    './bank_nifty_data/banknifty.csv', header=True, inferSchema=True)

read_df.show()
