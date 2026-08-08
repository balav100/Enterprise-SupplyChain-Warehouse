import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Configure Hadoop home directory for Windows compatibility
hadoop_dir = os.path.abspath(os.path.join(project_root, "hadoop"))
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["hadoop.home.dir"] = hadoop_dir
os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildFactOrders")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


stg = spark.read.jdbc(
    JDBC_URL,
    "stg_supply_chain",
    properties=CONNECTION_PROPERTIES
)


dim_customer = spark.read.jdbc(
    JDBC_URL,
    "dim_customer",
    properties=CONNECTION_PROPERTIES
)


dim_product = spark.read.jdbc(
    JDBC_URL,
    "dim_product",
    properties=CONNECTION_PROPERTIES
)


dim_category = spark.read.jdbc(
    JDBC_URL,
    "dim_category",
    properties=CONNECTION_PROPERTIES
)


dim_shipping = spark.read.jdbc(
    JDBC_URL,
    "dim_shipping",
    properties=CONNECTION_PROPERTIES
)


dim_region = spark.read.jdbc(
    JDBC_URL,
    "dim_region",
    properties=CONNECTION_PROPERTIES
)


dim_date = spark.read.jdbc(
    JDBC_URL,
    "dim_date",
    properties=CONNECTION_PROPERTIES
)


fact_orders = (
    stg
    .join(
        dim_customer.select("customer_id"),
        on="customer_id",
        how="left"
    )
    .join(
        dim_product.select("product_id"),
        col("product_card_id") == col("product_id"),
        how="left"
    )
    .join(
        dim_category.select("category_id"),
        on="category_id",
        how="left"
    )
    .join(
        dim_shipping.select("shipping_id", "shipping_mode", "delivery_status"),
        on=["shipping_mode", "delivery_status"],
        how="left"
    )
    .join(
        dim_region.select("region_id", "market", col("region").alias("order_region")),
        on=["market", "order_region"],
        how="left"
    )
    .withColumn(
        "order_date",
        to_date(
            to_timestamp(
                col("order_date"),
                "M/d/yyyy H:mm"
            )
        )
    )
)


fact_orders = fact_orders.select(
    "order_id",
    "customer_id",
    "product_id",
    "category_id",
    "shipping_id",
    "region_id",
    "order_date",
    col("order_item_quantity").alias("quantity"),
    col("sales"),
    col("order_item_discount").alias("discount"),
    col("order_profit_per_order").alias("profit"),
    col("order_item_profit_ratio").alias("profit_ratio"),
    "delivery_status",
    "late_delivery_risk"
)


print("Fact rows:", fact_orders.count())


fact_orders.write \
    .option("truncate", "true") \
    .jdbc(
        JDBC_URL,
        "fact_orders",
        mode="overwrite",
        properties=CONNECTION_PROPERTIES
    )

print("fact_orders loaded successfully")


spark.stop()