from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utilities import utils

catalog = "bronze"
schema = "kaggle"
files_storage = "abfss://landing@salesmarketingstorage1.dfs.core.windows.net/"


def ingest_csv_from_landing(folder: str):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", "\"")
        .option("mode", "PERMISSIVE")
        .load(f"{files_storage}/{folder}/")
    )


@dp.table(
    name=f"{catalog}.{schema}.dp_customers",
    comment="Raw customer data ingested from CSV via Auto Loader"
)
def bronze_customers():
    return (ingest_csv_from_landing("customers")) 


@dp.table(
    name=f"{catalog}.{schema}.dp_geolocation",
    comment="Raw geolocation data ingested from CSV via Auto Loader"
)
def bronze_geolocation():
    return (ingest_csv_from_landing("geolocations"))


@dp.table(
    name=f"{catalog}.{schema}.dp_order_items",
    comment="Raw order_items data ingested from CSV via Auto Loader"
)
def order_items():
    return (ingest_csv_from_landing("order_items"))


@dp.table(
    name=f"{catalog}.{schema}.dp_payments",
    comment="Raw payments data ingested from CSV via Auto Loader"
)
def payments():
    return (ingest_csv_from_landing("order_payments"))


@dp.table(
    name=f"{catalog}.{schema}.dp_order_reviews",
    comment="Raw order_reviews data ingested from CSV via Auto Loader"
)
def order_reviews():
    return (ingest_csv_from_landing("order_reviews"))


@dp.table(
    name=f"{catalog}.{schema}.dp_orders",
    comment="Raw orders data ingested from CSV via Auto Loader"
)
def orders():
    return (ingest_csv_from_landing("orders"))

@dp.table(
    name=f"{catalog}.{schema}.dp_products",
    comment="Raw products data ingested from CSV via Auto Loader"
)
def products():
    return (ingest_csv_from_landing("products"))
    

@dp.table(
    name=f"{catalog}.{schema}.dp_sellers",
    comment="Raw sellers data ingested from CSV via Auto Loader"
)
def sellers():
    return (ingest_csv_from_landing("sellers"))


@dp.table(
    name=f"{catalog}.{schema}.dp_product_category_name_translation",
    comment="Raw product_category_name_translation data ingested from CSV via Auto Loader"
)
def product_category_name_translation():
    return (ingest_csv_from_landing("category_name"))


