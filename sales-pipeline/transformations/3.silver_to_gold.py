from pyspark import pipelines as dp
from pyspark.sql.functions import xxhash64, col, current_date, lit

bronze_schema = "bronze.kaggle"
silver_schema = "silver.sales"
gold_schema = "gold.sales"
files_storage = "abfss://landing@salesmarketingstorage1.dfs.core.windows.net/"


dp.create_streaming_table(
    name=f"{gold_schema}.dim_customers",
    comment="Customer dimension table with SCD Type 2 tracking"
)


@dp.table(
    name=f"{silver_schema}.customers_for_scd",
    comment="Prepared customer data for SCD2 processing"
)
def customers_for_scd():
    return (
        dp.read(f"{silver_schema}.dp_customers_cleaned")
          .withColumn("customer_sk", xxhash64(col("customer_id")))
    )


dp.apply_changes(
    name="dim_customers_scd2",
    target=f"{gold_schema}.dim_customers",
    source=f"{silver_schema}.customers_for_scd",
    keys=["customer_unique_id"],  
    sequence_by="ingest_time",  
    stored_as_scd_type="2", 
    track_history_column_list=[
        "customer_id",
        "customer_zip_code_prefix", 
        "customer_city",
        "customer_state",
        "geolocation_sk"
    ] 
)
  

@dp.table(
    name=f"{gold_schema}.dim_orders",
    comment="Orders dimension table"
)
def orders():
    return (
        dp.read(f"{silver_schema}.dp_orders_cleaned")
          .withColumn("order_sk", xxhash64(col("order_id")))
    )


dp.create_streaming_table(
    name=f"{gold_schema}.dim_sellers",
    comment="Sellers dimension table with SCD Type 2 tracking"
)


@dp.table(
    name=f"{silver_schema}.sellers_for_scd",
    comment="Prepared sellers data for SCD2 processing"
)
def sellers_for_scd():
    return (
        dp.read(f"{silver_schema}.dp_sellers_cleaned")
          .withColumn("seller_sk", xxhash64(col("seller_id")))
    )


dp.apply_changes(
    name="dim_sellers_scd2",
    target=f"{gold_schema}.dim_sellers",
    source=f"{silver_schema}.sellers_for_scd",
    keys=["seller_id"],  
    sequence_by="ingest_time",  
    stored_as_scd_type="2", 
    track_history_column_list=[
        "seller_zip_code_prefix", 
        "seller_city",
        "seller_state",
        "geolocation_sk"
    ] 
)