from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, to_date, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from datetime import timedelta

# Paths
INPUT_PATH = "data/raw"
OUTPUT_PATH = "data/curated"

spark = SparkSession.builder \
    .appName("GMV Daily Snapshot ETL") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

def read_events(table_name: str):
    """ Read events from source files. """
    file_map = {
        'purchase': 'purchase.csv',
        'product_item': 'product_item.csv',
        'purchase_extra_info': 'purchase_extra_info.csv'
    }
    
    df = spark.read.csv(f"{INPUT_PATH}/{file_map[table_name]}", header=True, inferSchema=True)
    df = df.withColumn("transaction_datetime", col("transaction_datetime").cast(TimestampType())) \
           .withColumn("transaction_date", to_date(col("transaction_date")))
    
    if table_name == 'purchase':
        df = df.withColumn("order_date", to_date(col("order_date"))) \
               .withColumn("release_date", to_date(col("release_date")))
    return df


def get_latest_event_per_purchase(df, event_date, cols: list):
    """ For each purchase_id, returns the latest event of the day. """
    day_events = df.filter(col("transaction_date") == event_date)
    
    if day_events.count() == 0:
        return spark.createDataFrame([], StructType(
            [StructField("purchase_id", IntegerType(), True)] + 
            [StructField(c, StringType(), True) for c in cols]
        ))
    
    window = Window.partitionBy("purchase_id").orderBy(col("transaction_datetime").desc())
    return day_events.withColumn("rn", row_number().over(window)) \
                     .filter(col("rn") == 1) \
                     .select("purchase_id", *cols)


def get_previous_state(snapshot_df, purchase_ids: list, before_date):
    """ Search for the most recent state of each purchase BEFORE a date (carry forward). """
    if snapshot_df is None or not purchase_ids or snapshot_df.count() == 0:
        return None
    
    previous = snapshot_df.filter(
        (col("snapshot_date") < before_date) & 
        (col("purchase_id").isin(purchase_ids))
    )
    
    if previous.count() == 0:
        return None
    
    window = Window.partitionBy("purchase_id").orderBy(col("snapshot_date").desc())
    return previous.withColumn("rn", row_number().over(window)) \
                   .filter(col("rn") == 1) \
                   .drop("rn")


def consolidate_events(snapshot_date, transaction_date, purchase_latest, product_latest, extra_latest, previous_snapshot):
    """ Consolidate events from the 3 tables into a single row per purchase_id. """

    # Collect all purchases with event on the day
    all_purchase_ids = []
    for df in [purchase_latest, product_latest, extra_latest]:
        if df.count() > 0:
            all_purchase_ids.extend(df.select("purchase_id").rdd.flatMap(lambda x: x).collect())
    all_purchase_ids = list(set(all_purchase_ids))
    
    if not all_purchase_ids:
        return None
    
    # Base DataFrame with the PKs (snapshot_date, purchase_id)
    result = spark.createDataFrame([(pid,) for pid in all_purchase_ids], ["purchase_id"]) \
                  .withColumn("transaction_date", lit(transaction_date).cast(DateType())) \
                  .withColumn("snapshot_date", lit(snapshot_date).cast(DateType()))
    
    # Merge with events of the day
    for df in [purchase_latest, product_latest, extra_latest]:
        if df.count() > 0:
            result = result.join(df, on="purchase_id", how="left")
    
    # Fill gaps with previous state
    previous_state = get_previous_state(previous_snapshot, all_purchase_ids, snapshot_date)
    if previous_state is not None:
        fill_cols = ['buyer_id', 'producer_id', 'order_date', 'release_date', 
                     'product_id', 'item_quantity', 'purchase_value', 'subsidiary']
        prev_cols = [col(c).alias(f"prev_{c}") for c in fill_cols if c in previous_state.columns]
        
        if prev_cols:
            prev_map = previous_state.select("purchase_id", *prev_cols)
            result = result.join(prev_map, on="purchase_id", how="left")
            
            for col_name in fill_cols:
                prev_col = f"prev_{col_name}"
                if prev_col in result.columns:
                    if col_name in result.columns:
                        result = result.withColumn(col_name, coalesce(col(col_name), col(prev_col)))
                    else:
                        result = result.withColumnRenamed(prev_col, col_name)
                    if prev_col in result.columns:
                        result = result.drop(prev_col)
    
    # Enforce the schema
    expected_columns = ['snapshot_date', 'transaction_date', 'purchase_id', 'buyer_id', 
                        'producer_id', 'order_date', 'release_date', 'product_id', 
                        'item_quantity', 'purchase_value', 'subsidiary']
    for col_name in expected_columns:
        if col_name not in result.columns:
            result = result.withColumn(col_name, lit(None))
    
    # Add flags
    result = result.withColumn("is_current", lit(True)) \
                   .withColumn("is_paid", col("release_date").isNotNull())
    
    return result.select(*expected_columns, "is_current", "is_paid")


def process_daily_snapshot(processing_date, snapshot_df):
    """ Process D-1: events of yesterday become today's snapshot. """
    event_date = processing_date - timedelta(days=1)
    
    purchase_latest = get_latest_event_per_purchase(
        read_events('purchase'), event_date, 
        ['buyer_id', 'prod_item_id', 'order_date', 'release_date', 'producer_id']
    )
    product_latest = get_latest_event_per_purchase(
        read_events('product_item'), event_date,
        ['product_id', 'item_quantity', 'purchase_value']
    )
    extra_latest = get_latest_event_per_purchase(
        read_events('purchase_extra_info'), event_date,
        ['subsidiary']
    )
    
    return consolidate_events(processing_date, event_date, purchase_latest, 
                              product_latest, extra_latest, snapshot_df)


def update_is_current(full_snapshot):
    """ Update is_current to TRUE only for the latest snapshot of each purchase. """
    if full_snapshot is None or full_snapshot.count() == 0:
        return full_snapshot
    
    window = Window.partitionBy("purchase_id").orderBy(col("snapshot_date").desc())
    return full_snapshot.withColumn("rn", row_number().over(window)) \
                        .withColumn("is_current", col("rn") == 1) \
                        .drop("rn")


def run_etl():
    """ Execute ETL for all days with events. """
    
    # Find all event dates
    all_dates = read_events('purchase').select("transaction_date").union(
        read_events('product_item').select("transaction_date")
    ).union(
        read_events('purchase_extra_info').select("transaction_date")
    ).distinct().orderBy("transaction_date").collect()
    
    sorted_dates = [row['transaction_date'] for row in all_dates]
    
    # Process day by day
    full_snapshot = None
    for event_date in sorted_dates:
        processing_date = event_date + timedelta(days=1)
        new_snapshot = process_daily_snapshot(processing_date, full_snapshot)
        
        if new_snapshot is not None:
            full_snapshot = new_snapshot if full_snapshot is None else full_snapshot.union(new_snapshot)
    
    full_snapshot = update_is_current(full_snapshot)
    
    # Drop duplicates
    full_snapshot = full_snapshot.dropDuplicates(["snapshot_date", "purchase_id"])

    # Save dataset
    full_snapshot.write.mode("overwrite") \
                 .partitionBy("snapshot_date") \
                 .parquet(f"{OUTPUT_PATH}/gmv_daily_snapshot")

    return full_snapshot


if __name__ == "__main__":
    run_etl()
    spark.stop()