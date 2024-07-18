from dagster import Definitions, define_asset_job, AssetSelection, build_schedule_from_partitioned_job, DailyPartitionsDefinition
from dagster_module.io_manager import parquet_io_manager 
from dagster_module.assets import determine_partition_range, load_daily_partition
import os 

DATA_BASE_PATH = os.getenv("DATA_BASE_PATH", "data/")

determine_range_job = define_asset_job(
    name="determine_range_job",
    selection=AssetSelection.assets(determine_partition_range),
)

daily_partitions_job = define_asset_job(
    name="daily_partitions_job",
    selection=AssetSelection.assets(load_daily_partition),
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01")
)

daily_schedule = build_schedule_from_partitioned_job(
    job=daily_partitions_job,
    name="daily_schedule",
)

defs = Definitions(
    assets=[determine_partition_range, load_daily_partition],
    resources={"io_manager": parquet_io_manager.configured({"base_path": DATA_BASE_PATH})},
    jobs=[determine_range_job, daily_partitions_job],
    schedules=[daily_schedule],
)
