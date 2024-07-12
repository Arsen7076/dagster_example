import sys
import os
from datetime import datetime
from dagster import DailyPartitionsDefinition, DagsterInstance, PartitionKeyRange, materialize

# Add the parent directory of 'dagster_module' to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_module.assets import determine_partition_range, create_backfill_asset

if __name__ == "__main__":
    # Get an instance of Dagster
    inst = DagsterInstance.get()

    # First, determine the partition range
    result = materialize(
        partition_key=None,
        instance=inst,
        assets=[determine_partition_range],  # Pass the actual asset
    )

    # Extract min_date and max_date from the result
    min_date = result.asset_value(['determine_partition_range'])["min_date"]
    max_date = result.asset_value(['determine_partition_range'])["max_date"]
    
    print(f"Min date: {min_date}, Max date: {max_date}")

    if not min_date or not max_date:
        raise ValueError("Failed to determine partition range")

    # Define the partition definition for backfilling based on the determined date range
    partitions_def = DailyPartitionsDefinition(start_date=min_date)

    # Create the backfill asset with the dynamic partitions definition
    backfill_historical_data = create_backfill_asset(partitions_def)

    # Get all partition keys in the determined range
    partition_keys = partitions_def.get_partition_keys_in_range(PartitionKeyRange(min_date, max_date))

    # Trigger backfill for each partition key
    for partition_key in partition_keys:
        print(f"Materializing partition key: {partition_key}")
        materialize(
            assets=[backfill_historical_data],  # Pass the actual asset
            partition_key=partition_key,
            instance=inst,
        )
