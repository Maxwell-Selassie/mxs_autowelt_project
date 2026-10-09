# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from functools import reduce


# ---------------------------------------------------------
# 1. List your Bronze tables
# ---------------------------------------------------------

bronze_tables = [
    "bronze_branches",
    "bronze_countries", 
    "bronze_employees", 
    "bronze_historical_deliveries", 
    "bronze_historical_orders_snapshot", 
    "bronze_historical_payments", 
    "bronze_regions", 
    "bronze_vehicles", 
    "bronze_customers"
]
catalog = "mxs_autowelt_project"
schema = "mxs_autowelt_bronze"

# ---------------------------------------------------------
# 2. Table-level profiling
# ---------------------------------------------------------

def profile_table(table_name):

    df = spark.table(f"{catalog}.{schema}.{table_name}")

    row_count = df.count()
    column_count = len(df.columns)

    # Count completely duplicated rows
    duplicate_count = (
        row_count
        - df.dropDuplicates().count()
    )

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "duplicate_count": duplicate_count,
    }


table_profiles = [
    profile_table(table)
    for table in bronze_tables
]

table_profile_df = spark.createDataFrame(table_profiles)

display(table_profile_df)

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

catalog = "mxs_autowelt_project"
schema = "mxs_autowelt_bronze"


def profile_table_columns(table_name):

    df = spark.table(f"{catalog}.{schema}.{table_name}")

    row_count = df.count()

    column_profiles = []

    for field in df.schema.fields:

        column = field.name

        profile = (
            df.select(
                F.lit(table_name).alias("table_name"),
                F.lit(column).alias("column_name"),
                F.lit(str(field.dataType)).alias("data_type"),

                F.lit(row_count).alias("row_count"),

                F.count(
                    F.when(F.col(column).isNull(), 1)
                ).alias("null_count"),

                F.countDistinct(
                    F.col(column)
                ).alias("distinct_count"),

                # Cast to string so every table has compatible types
                F.min(F.col(column))
                 .cast("string")
                 .alias("min_value"),

                F.max(F.col(column))
                 .cast("string")
                 .alias("max_value"),
            )
            .withColumn(
                "null_percentage",
                F.round(
                    F.col("null_count")
                    / F.col("row_count")
                    * 100,
                    2
                )
            )
            .withColumn(
                "distinct_percentage",
                F.round(
                    F.col("distinct_count")
                    / F.col("row_count")
                    * 100,
                    2
                )
            )
        )

        column_profiles.append(profile)

    return reduce(
        lambda a, b: a.unionByName(b),
        column_profiles
    )


profiles = [
    profile_table_columns(table)
    for table in bronze_tables
]

bronze_column_profile = reduce(
    lambda a, b: a.unionByName(b),
    profiles
)

display(bronze_column_profile)