from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder \
    .appName("spark") \
    .master("local[*]")\
    .getOrCreate()

# Read data from CSV
# df = spark.read.csv("raw.csv", header=True)

# # JDBC URL to connect to Redshift
# jdbc_url = "jdbc:redshift://redshift-cluster-url:5439/database_name"

# # Redshift credentials
# user = "your_redshift_username"
# password = "your_redshift_password"
# aws_iam_role = "your_aws_iam_role"

# # Define the table to read from
# read_table_name = "your_table_name"
# write_table_name = "your_table_name"


# read_df = spark.read.format("jdbc") \
# .option("url", "jdbc:redshift://your_redshift_endpoint:your_redshift_port/your_database") \
# .option("dbtable", "your_table") \
# .option("user", "your_redshift_username") \
# .option("password", "your_redshift_password") \
# .load()

# Get the max timestamp from the 'updated_at' column
# max_timestamp_df = read_df.select(max(col("updated_at")).alias("max_timestamp"))

# # Show the max timestamp
# max_timestamp = max_timestamp_df.collect()[0]["max_timestamp"]
# print("Max Timestamp:", max_timestamp)

# # Filter records based on the timestamp column for incremental run
# input_df = read_df.filter(col("updated_at") > last_run_timestamp)


# # Apply transformations similar to the provided SQL logic
# input_df = df.filter(
#     (col("data_header") == "Plant Run") &
#     (col("data_type") == "Association") &
#     (col("entity_type") == "SEED TRAIN LOT") &
#     (col("active") == 1) &
#     (col("delete_flag") == 0)
# ).select(
#     col("string_data").alias("plantrun_barcode"),
#     col("barcode").alias("harvest_lot_barcode"),
#     lit("PLANT RUN -> SEED LOT").alias("connection_type")
# )

# input_df = input_df.union(
#     df.filter(
#         (col("data_header") == "Seed Train Or Production Sample Lot") &
#         (col("data_type") == "Association") &
#         (col("entity_type") == "PLANT RUN") &
#         (col("casted_data_type") == "SEED TRAIN LOT") &
#         (col("active") == 1) &
#         (col("delete_flag") == 0)
#     ).select(
#         col("string_data").alias("plantrun_barcode"),
#         col("barcode").alias("harvest_lot_barcode"),
#         lit("SEED LOT -> PLANT RUN").alias("connection_type")
#     )
# )

# input_df = input_df.union(
#     df.filter(
#         (col("data_header") == "Seed Train Lot") &
#         (col("data_type") == "Association") &
#         (col("entity_type") == "PRODUCTION LOT") &
#         (col("active") == 1) &
#         (col("delete_flag") == 0)
#     ).select(
#         col("string_data").alias("plantrun_barcode"),
#         col("barcode").alias("harvest_lot_barcode"),
#         lit("SEED LOT -> PRODUCTION LOT").alias("connection_type")
#     )
# )

input_df = spark.read.csv("result.csv", header=True)

input_df = input_df.withColumnRenamed("HARVESTLOTBARCODE", "harvest_lot_barcode") \
    .withColumnRenamed("PLANTRUNBARCODE", "plantrun_barcode")

pd_subquery = input_df.filter(input_df["harvest_lot_barcode"].startswith("PD")).select(
    col("plantrun_barcode").alias("pd_plantrun_barcode"),
    col("harvest_lot_barcode").alias("pd_lot_barcode")
)

# Perform left join with the main DataFrame
final_df = input_df.join(pd_subquery, pd_subquery.pd_plantrun_barcode == input_df.harvest_lot_barcode, "left_outer").select(
    input_df["*"],
    pd_subquery["pd_lot_barcode"],
    split(input_df["harvest_lot_barcode"], "-")[0].alias("hv_lot"))

cross_join_condition = col("f1.hv_lot") == col("f2.hv_lot")

# Define cross join output
cross_join_output = final_df.alias("f1").crossJoin(
    final_df.select("plantrun_barcode", "hv_lot").distinct().alias("f2")
).filter(cross_join_condition).select(
    col("f1.plantrun_barcode"),
    col("f1.harvest_lot_barcode"),
    col("f1.pd_lot_barcode"),
    col("f1.hv_lot"),
    col("f2.plantrun_barcode").alias("passed_plantrun")
)

cross_join_output_filtered = cross_join_output.filter(col("pd_lot_barcode").isNotNull()).select(
    col("plantrun_barcode").alias("last_plantrun"),
    col("harvest_lot_barcode").alias("last_seed_train_lot"),
    col("hv_lot").alias("filter_hv_lot")
).distinct()

pd_join_output = cross_join_output.join(
    cross_join_output_filtered, cross_join_output.hv_lot == cross_join_output_filtered.filter_hv_lot, how='left').select(
    cross_join_output.passed_plantrun,
    cross_join_output_filtered.last_seed_train_lot,
    cross_join_output_filtered.last_plantrun,
    cross_join_output.plantrun_barcode,
    cross_join_output.harvest_lot_barcode,
    cross_join_output.hv_lot
).withColumn("passed_plantrun_type", lit('SEED TRAIN').cast("string")) \
 .withColumn("plantrun_type", lit('SEED TRAIN').cast("string"))

window_spec = Window.orderBy("passed_plantrun", "last_seed_train_lot",
                             "last_plantrun", "plantrun_barcode", "harvest_lot_barcode", "hv_lot")

# Add the 'novaflex_data_key' column using row_number() window function
pd_join_output = pd_join_output.withColumn(
    "novaflex_data_key", row_number().over(window_spec))

lineage_df = pd_join_output.select(
    col("passed_plantrun"), col("plantrun_barcode")).distinct()

window_spec = Window.orderBy("passed_plantrun", "plantrun_barcode")

# Add the 'novaflex_data_key' column using row_number() window function
lineage_df = lineage_df.withColumn(
    "plant_datakey", row_number().over(window_spec))

# Add a new column to mark rows where passed_plantrun = plantrun_barcode
lineage_rank_df = lineage_df.withColumn("lineage_rank",
                                        when(col("passed_plantrun") ==
                                             col("plantrun_barcode"), 1)
                                        .otherwise(0))

#  Odd Rank Logic


def apply_logic_odd(df):

    # Window specification
    window_spec = Window.partitionBy(
        "passed_plantrun").orderBy("plant_datakey")

    # Apply the logic to all rows in the partition
    updated_df = df.withColumn("lineage_rank",
                               when(lag("lineage_rank").over(window_spec) >= 1,
                                    lag("lineage_rank").over(window_spec) + 2)
                               .otherwise(col("lineage_rank")))

    # Check for convergence
    if updated_df.subtract(df).isEmpty():
        return updated_df  # Convergence reached
    else:
        return apply_logic_odd(updated_df)  # Recursively apply logic


# Apply the recursive function to the initial DataFrame
odd_rank_df = apply_logic_odd(lineage_rank_df)

odd_rank_df_final = odd_rank_df.filter(col("lineage_rank") >= 1).withColumn(
    'lineage_entity', lit('Downstream'))

# Even Rank Logic

# Define a recursive function to apply the logic until convergence


def apply_logic_even(df):
    # Define a window specification ordered by some column in descending order
    window_spec = Window.partitionBy(
        "passed_plantrun").orderBy(col("plant_datakey").desc())

    # Apply the logic to all rows in the partition
    updated_df = df.withColumn("lineage_rank",
                               when(lag("lineage_rank").over(window_spec) == 1,
                                    lag("lineage_rank").over(window_spec) + 1)
                               .when(lag("lineage_rank").over(window_spec) > 1,
                                     lag("lineage_rank").over(window_spec) + 2)
                               .otherwise(col("lineage_rank")))

    # Check for convergence
    if updated_df.subtract(df).isEmpty():
        return updated_df  # Convergence reached
    else:
        return apply_logic_even(updated_df)  # Recursively apply logic


even_rank_df = odd_rank_df.filter(col("lineage_rank") <= 1)

even_rank_df_final = apply_logic_even(even_rank_df)

even_rank_df_final = even_rank_df_final.filter(
    col('lineage_rank') > 1).withColumn('lineage_entity', lit('Upstream'))

lineage_final_rank_df = odd_rank_df_final.union(even_rank_df_final).dropDuplicates(
).orderBy(col('passed_plantrun'), col('plant_datakey'))

final_output_df = pd_join_output.join(lineage_final_rank_df, (pd_join_output.passed_plantrun == lineage_final_rank_df.passed_plantrun) & (
    pd_join_output.plantrun_barcode == lineage_final_rank_df.plantrun_barcode), how='left').select(pd_join_output['*'], lineage_final_rank_df['lineage_rank'], lineage_final_rank_df['lineage_entity'])

# pandas_df = final_output_df.toPandas()

# # Write Pandas DataFrame to CSV
# pandas_df.to_csv('output.csv', index=False)

# Write data to Redshift
# final_output_df.write \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", write_table_name) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("aws_iam_role", aws_iam_role) \
#     .mode("append") \
#     .save()
