import sys
import os
from datetime import datetime
from dagster import DailyPartitionsDefinition, DagsterInstance, PartitionKeyRange, materialize

# Add the parent directory of 'dagster_module' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_module.assets import determine_partition_range, create_backfill_asset

if __name__ == "__main__":
    inst = DagsterInstance.get()

    result = materialize(
        partition_key=None,
        instance=inst,
        assets=[determine_partition_range],
    )

    min_date = result.asset_value('determine_partition_range')["min_date"]
    max_date = result.asset_value('determine_partition_range')["max_date"]

    print(f"Min date: {min_date}, Max date: {max_date}")

    if not min_date or not max_date:
        raise ValueError("Failed to determine partition range")

    partitions_def = DailyPartitionsDefinition(start_date=min_date)
    backfill_historical_data = create_backfill_asset(partitions_def)

    partition_keys = partitions_def.get_partition_keys_in_range(PartitionKeyRange(min_date, max_date))

    for partition_key in partition_keys:
        print(f"Materializing partition key: {partition_key}")
        materialize(
            assets=[backfill_historical_data],
            partition_key=partition_key,
            instance=inst,
        )
