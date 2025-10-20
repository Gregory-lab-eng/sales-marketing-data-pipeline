from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, xxhash64, col


bronze_schema = "bronze.kaggle"
silver_schema = "silver.sales"
files_storage = "abfss://landing@salesmarketingstorage1.dfs.core.windows.net/"


@dp.table(
    name=f"{silver_schema}.dp_geolocations_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_zip_code": "geolocation_zip_code_prefix IS NOT NULL"}
)
def geolocations_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_geolocation")
        .drop("_rescued_data")
        .withColumn("geolocation_sk", xxhash64(col("geolocation_zip_code_prefix")))
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_customers_cleaned_base",
    comment="Filtered and deduplicated customers from bronze"
)
def customers_cleaned_base():
    return (
        dp.read_stream(f"{bronze_schema}.dp_customers")
        .filter("customer_unique_id IS NOT NULL")
        .dropDuplicates(["customer_unique_id", "customer_id"])
        .withColumn("ingest_time", current_timestamp())
        .drop("_rescued_data")
    )


@dp.table(
    name=f"{silver_schema}.dp_customers_cleaned",
    comment="Customers enriched with geolocation surrogate keys"
)
def customers_cleaned():
    customers = (dp
        .read_stream(f"{silver_schema}.dp_customers_cleaned_base")
    )
    geos = (dp
        .read(f"{silver_schema}.dp_geolocations_cleaned")
    )

    return (
        customers.join(
            geos,
            customers["customer_zip_code_prefix"] == geos["geolocation_zip_code_prefix"],
            "left"
        )
        .select(
            customers["*"],
            geos["geolocation_sk"]
        )
    )


@dp.table(
    name=f"{silver_schema}.dp_order_items_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_order_id": "order_id IS NOT NULL"}
)
def order_items_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_order_items")
        .drop("_rescued_data")
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_payments_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_order_id": "order_id IS NOT NULL"}
)
def payments_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_payments")
        .drop("_rescued_data")
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_order_reviews_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_order_id": "order_id IS NOT NULL",
     "valid_review_id": "review_id IS NOT NULL"}
)
def order_reviews_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_order_reviews")
        .drop("_rescued_data")
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_orders_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_order_id": "order_id IS NOT NULL"}
)
def orders_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_orders")
        .dropDuplicates(["order_id"])
        .drop("_rescued_data")
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_products_cleaned"
)
@dp.expect_all_or_drop(
    {"valid_product_id": "product_id IS NOT NULL"}
)
def products_cleaned():
    return (
        dp.read_stream(f"{bronze_schema}.dp_products")
        .dropDuplicates(["product_id"])
        .drop("_rescued_data")
        .withColumn("ingest_time", current_timestamp())
    )


@dp.table(
    name=f"{silver_schema}.dp_sellers_cleaned_base",
    comment="Filtered and deduplicated sellers from bronze"
)
def sellers_cleaned_base():
    return (
        dp.read_stream(f"{bronze_schema}.dp_sellers")
        .filter("seller_id IS NOT NULL")
        .dropDuplicates(["seller_id"])
        .withColumn("ingest_time", current_timestamp())
        .drop("_rescued_data")
    )


@dp.table(
    name=f"{silver_schema}.dp_sellers_cleaned",
    comment="Sellers enriched with geolocation surrogate keys"
)
def sellers_cleaned():
    sellers = (dp
        .read_stream(f"{silver_schema}.dp_sellers_cleaned_base")
    )
    geos = (dp
        .read(f"{silver_schema}.dp_geolocations_cleaned")
    )

    return (
        sellers.join(
            geos,
            sellers["seller_zip_code_prefix"] == geos["geolocation_zip_code_prefix"],
            "left"
        )
        .select(
            sellers["*"],
            geos["geolocation_sk"]
        )
    )