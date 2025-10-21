from pyspark import pipelines as dp
from pyspark.sql.functions import xxhash64, col, concat_ws,concat,  monotonically_increasing_id, coalesce, lit, sum, count

bronze_schema = "bronze.kaggle"
silver_schema = "silver.sales"
gold_schema = "gold.sales"


dp.create_streaming_table(
    name=f"{gold_schema}.dim_customers",
    comment="Customer dimension table with SCD Type 2 tracking"
)

@dp.table(
    name=f"{silver_schema}.customers_for_scd",
    comment="Prepared customer data for SCD2 processing"
)
def customers_for_scd():
    return dp.read(f"{silver_schema}.dp_customers_cleaned")

dp.apply_changes(
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


dp.create_streaming_table(
    name=f"{gold_schema}.dim_sellers",
    comment="Sellers dimension table with SCD Type 2 tracking"
)

@dp.table(
    name=f"{silver_schema}.sellers_for_scd",
    comment="Prepared sellers data for SCD2 processing"
)
def sellers_for_scd():
    return dp.read(f"{silver_schema}.dp_sellers_cleaned")

dp.apply_changes(
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


@dp.table(
    name=f"{gold_schema}.dim_products",
    comment="Products dimension table"
)
def dim_products():
    return dp.read(f"{silver_schema}.dp_products_cleaned")


@dp.table(
    name=f"{gold_schema}.dim_orders",
    comment="Orders dimension table"
)
def dim_orders():
    return dp.read(f"{silver_schema}.dp_orders_cleaned")


@dp.table(
    name=f"{gold_schema}.dim_payments",
    comment="Payments dimension table"
)
def dim_payments():
    return dp.read(f"{silver_schema}.dp_payments_cleaned")


@dp.table(
    name=f"{gold_schema}.dim_reviews",
    comment="Reviews dimension table"
)
def dim_reviews():
    return dp.read(f"{silver_schema}.dp_order_reviews_cleaned")



# FACT TABLE
@dp.table(
    name=f"{gold_schema}.fct_order_items",
    comment="Order items fact table with proper SCD2 dimension joins"
)
def fct_order_items():
    items = dp.read(f"{silver_schema}.dp_order_items_cleaned")
    
    orders = dp.read(f"{gold_schema}.dim_orders")
    
    # Join with payments (aggregate first)
    payments = (
        dp.read(f"{gold_schema}.dim_payments")
        .groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_payment_value"),
            count("*").alias("payment_count")
        )
    )
    
    reviews = dp.read(f"{gold_schema}.dim_reviews")
    
    sellers = (
        dp.read(f"{gold_schema}.dim_sellers")
        .filter(col("__END_AT").isNull())  
        .withColumn(
            "seller_sk",
            xxhash64(concat(col("seller_id"), col("__START_AT").cast("string")))
        )
    )
    
    products = dp.read(f"{gold_schema}.dim_products")
    
    customers = (
        dp.read(f"{gold_schema}.dim_customers")
        .filter(col("__END_AT").isNull()) 
        .withColumn(
            "customer_sk",
            xxhash64(concat(col("customer_unique_id"), col("__START_AT").cast("string")))
        )
    )
    
    return (
        items
        .join(orders, "order_id")
        .join(payments, "order_id", "left")
        .join(reviews, "order_id", "left")
        .join(sellers, "seller_id")
        .join(products, "product_id")
        .join(customers, orders.customer_id == customers.customer_id)
        .select(
            col("customer_sk"),
            col("seller_sk"),
            col("order_id"),
            col("product_id"),
            col("price"),
            col("freight_value"),
            coalesce(col("total_payment_value"), lit(0.0)).alias("total_payment_value"),
            col("order_purchase_timestamp"),
            col("order_item_id")
        )
    )