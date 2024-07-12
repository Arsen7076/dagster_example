from dagster import Definitions, define_asset_job, AssetSelection, build_schedule_from_partitioned_job, DailyPartitionsDefinition
from dagster_module.io_manager import parquet_io_manager 
from dagster_module.assets import determine_partition_range, load_daily_partition
import os 

# Base path for data files
DATA_BASE_PATH = os.getenv("DATA_BASE_PATH", "data/")

# Define a job to determine the range of partitions
determine_range_job = define_asset_job(
    name="determine_range_job",
    selection=AssetSelection.assets(determine_partition_range),
)

# Define a daily partitioned job to load daily data
daily_partitions_job = define_asset_job(
    name="daily_partitions_job",
    selection=AssetSelection.assets(load_daily_partition),
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01")
)

# Define a daily schedule for the partitioned job
daily_schedule = build_schedule_from_partitioned_job(
    job=daily_partitions_job,
    name="daily_schedule",
)

# Define Dagster definitions including assets, resources, jobs, and schedules
defs = Definitions(
    assets=[determine_partition_range, load_daily_partition],
    resources={"io_manager": parquet_io_manager.configured({"base_path": DATA_BASE_PATH})},
    jobs=[determine_range_job, daily_partitions_job],
    schedules=[daily_schedule],
)
