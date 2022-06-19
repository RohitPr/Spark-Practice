from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd


spark = SparkSession \
    .builder \
    .appName('Spark') \
    .master('local[*]') \
    .config('spark.sql.analyzer.failAmbiguousSelfJoin', False).getOrCreate()

read_df = spark.read.csv(
    './adult/adult.csv', header=True, inferSchema=True)

print(read_df.columns)

read_df = read_df.select('age', 'workclass', 'fnlwgt', 'education', 'country')

# windowSpec  = window.partitionBy("country").orderBy("age")

read_df = read_df.withColumn('new_date', rank().over(
    Window.partitionBy('country').orderBy('age'))).show()
