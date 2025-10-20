from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utilities import utils


@dp.table(
    name="bronze.kaggle.dp_customers",
    comment="Raw customer data ingested from CSV via Auto Loader"
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("mode", "PERMISSIVE")
        .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/customers/")
    )


@dp.table(
    name="bronze.kaggle.dp_geolocation",
    comment="Raw geolocation data ingested from CSV via Auto Loader"
)
def bronze_geolocation():
    return (
    spark.readStream 
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/geolocations/")
    )


@dp.table(
    name="bronze.kaggle.dp_order_items",
    comment="Raw order_items data ingested from CSV via Auto Loader"
)
def order_items():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/order_items/")
    )


@dp.table(
    name="bronze.kaggle.dp_payments",
    comment="Raw payments data ingested from CSV via Auto Loader"
)
def payments():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/order_payments/")
    )


@dp.table(
    name="bronze.kaggle.dp_order_reviews",
    comment="Raw order_reviews data ingested from CSV via Auto Loader"
)
def order_reviews():
    return(
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/order_reviews/")
    )


@dp.table(
    name="bronze.kaggle.dp_orders",
    comment="Raw orders data ingested from CSV via Auto Loader"
)
def orders():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/orders/")
    )

@dp.table(
    name="bronze.kaggle.dp_products",
    comment="Raw products data ingested from CSV via Auto Loader"
)
def products():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/products/")
)
    

@dp.table(
    name="bronze.kaggle.dp_sellers",
    comment="Raw sellers data ingested from CSV via Auto Loader"
)
def sellers():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/sellers/")
    )


@dp.table(
    name="bronze.kaggle.dp_product_category_name_translation",
    comment="Raw product_category_name_translation data ingested from CSV via Auto Loader"
)
def product_category_name_translation():
    return (
    spark.readStream 
    .format("cloudFiles").option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("quote", "\"")
    .option("mode", "PERMISSIVE")
    .load("abfss://landing@salesmarketingstorage1.dfs.core.windows.net/category_name/")
    )


