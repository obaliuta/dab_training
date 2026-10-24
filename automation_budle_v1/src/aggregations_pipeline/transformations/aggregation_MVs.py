from pyspark import pipelines as dp
from pyspark.sql import functions as F


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")


@dp.materialized_view
def year_month_summary():
    df = (
        spark.read.table(f"`{catalog}`.`{schema}`.daily_stats")
        .groupBy(F.year(F.col("pickup_date")).alias("year"), F.month(F.col("pickup_date")).alias("month"))
        .agg(
            F.round(F.avg("avg_trip_distance"), 2).alias("avg_trip_distance"),
            F.sum("total_fare_amount").alias("total_fare_amount"),
        )
    )
    return df


@dp.materialized_view
def year_summary():
    df = (
        spark.read.table(f"`{catalog}`.`{schema}`.daily_stats")
        .groupBy(F.year(F.col("pickup_date")).alias("year"))
        .agg(
            F.round(F.avg("avg_trip_distance"), 2).alias("avg_trip_distance"),
            F.sum("total_fare_amount").alias("total_fare_amount"),
        )
    )
    return df