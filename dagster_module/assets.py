from dagster import asset, Output, AssetMaterialization, DailyPartitionsDefinition, AssetKey
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, to_date
import os

DATA_BASE_PATH = os.getenv("DATA_BASE_PATH", "data/")
YELLOW_DATA_FILE = os.getenv("YELLOW_DATA_FILE", "yellow_tripdata_2024-01.parquet")
GREEN_DATA_FILE = os.getenv("GREEN_DATA_FILE", "green_tripdata_2024-01.parquet")
YELLOW_DATE_COLUMN_NAME = os.getenv("YELLOW_DATE_COLUMN_NAME", "tpep_pickup_datetime")
GREEN_DATE_COLUMN_NAME = os.getenv("GREEN_DATE_COLUMN_NAME", "lpep_pickup_datetime")

@asset
def determine_partition_range(context):
    """
    Determines the partition range by finding the minimum and maximum dates in the dataset.
    """
    yellow_data_path = os.path.join(DATA_BASE_PATH, YELLOW_DATA_FILE)
    
    spark = SparkSession.builder.appName("Determine Partition Range").getOrCreate()
    try:
        df = spark.read.parquet(yellow_data_path)
        df = df.withColumn("date", to_date(df[YELLOW_DATE_COLUMN_NAME]))
        min_date = df.agg({'date': 'min'}).select("min(date)").collect()[0][0].strftime("%Y-%m-%d")
        max_date = df.agg({'date': 'max'}).select("max(date)").collect()[0][0].strftime("%Y-%m-%d")
        
        yield AssetMaterialization(
            asset_key=context.asset_key,
            description="Define range of date",
            metadata={"min_date": min_date, "max_date": max_date}
        )
        yield Output({"min_date": min_date, "max_date": max_date})
    finally:
        spark.stop()

def create_backfill_asset(partitions_def):
    @asset(partitions_def=partitions_def)
    def backfill_historical_data(context):
        partition_date = context.asset_partition_key_for_output()
        yellow_data_path = os.path.join(DATA_BASE_PATH, YELLOW_DATA_FILE)
        
        spark = SparkSession.builder.appName("Historical Data Backfill").getOrCreate()
        try:
            df = spark.read.parquet(yellow_data_path)
            df = df.withColumn("date", to_date(df[YELLOW_DATE_COLUMN_NAME]))
            partitioned_df = df.filter(df["date"] == partition_date)
            
            partitioned_df_path = os.path.join(DATA_BASE_PATH, f"{partition_date}.parquet")
            partitioned_df.write.parquet(partitioned_df_path)
        finally:
            spark.stop()
    
    return backfill_historical_data

@asset(partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"))
def load_daily_partition(context):
    partition_date = context.asset_partition_key_for_output()
    green_data_path = os.path.join(DATA_BASE_PATH, GREEN_DATA_FILE)
    
    spark = SparkSession.builder.appName("Daily Data Load").getOrCreate()
    try:
        df = spark.read.parquet(green_data_path)
        df = df.withColumn("date", to_date(df[GREEN_DATE_COLUMN_NAME]))
        partitioned_df = df.filter(df["date"] == partition_date)
        
        yield AssetMaterialization(
            asset_key=context.asset_key,
            description=f"Partitioned data for {partition_date}",
            metadata={"path": green_data_path}
        )
        yield Output(partitioned_df)
    finally:
        spark.stop()
