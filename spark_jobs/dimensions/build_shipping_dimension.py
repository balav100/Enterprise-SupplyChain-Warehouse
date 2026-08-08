import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    .appName("BuildShippingDimension")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


stg = spark.read.jdbc(
    url=JDBC_URL,
    table="stg_supply_chain",
    properties=CONNECTION_PROPERTIES
)


dim_shipping = (
    stg.select(
        "shipping_mode",
        "delivery_status",
        col("days_for_shipment_scheduled").alias("scheduled_days"),
        col("days_for_shipping_real").alias("actual_days")
    )
    .groupBy(
        "shipping_mode",
        "delivery_status"
    )
    .agg(
        first("scheduled_days").alias("scheduled_days"),
        first("actual_days").alias("actual_days")
    )
    .withColumn(
        "shipping_id",
        monotonically_increasing_id() + 1
    )
    .select(
        "shipping_id",
        "shipping_mode",
        "delivery_status",
        "scheduled_days",
        "actual_days"
    )
)


print("Shipping dimension rows:", dim_shipping.count())


dim_shipping.write \
    .option("truncate", "true") \
    .jdbc(
        JDBC_URL,
        "dim_shipping",
        mode="overwrite",
        properties=CONNECTION_PROPERTIES
    )


print("dim_shipping loaded successfully")


spark.stop()