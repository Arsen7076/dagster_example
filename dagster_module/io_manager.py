import os
from dagster import IOManager, io_manager, Field, String
from pyspark.sql import SparkSession

class ParquetIOManager(IOManager):
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.spark = SparkSession.builder.appName("DagsterParquetIO").getOrCreate()

    def handle_output(self, context, obj):
        try:
            partition_date = context.asset_partition_key
            if partition_date:
                path = os.path.join(self.base_path, partition_date)
            else:
                path = self.base_path
            os.makedirs(path, exist_ok=True)
            
            # Assume obj is a file path to the parquet data
            file_path = os.path.join(path, f"{context.step_key}.parquet")
            obj_df = self.spark.read.parquet(obj)
            obj_df.write.parquet(file_path)
        except Exception as e:
            print(f"Error handling output: {e}")
            print(context)

    def load_input(self, context):
        partition_date = context.asset_partition_key
        if partition_date:
            file_path = os.path.join(self.base_path, partition_date, f"{context.upstream_output.step_key}.parquet")
        else:
            file_path = os.path.join(self.base_path, f"{context.upstream_output.step_key}.parquet")
        return self.spark.read.parquet(file_path).toPandas()

@io_manager(config_schema={"base_path": Field(String)})
def parquet_io_manager(init_context):
    return ParquetIOManager(base_path=init_context.resource_config["base_path"])
