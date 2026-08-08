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
from pyspark.sql.functions import col
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildProductDimension")
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


dim_product = (
    stg.select(
        col("product_card_id").alias("product_id"),
        "product_name",
        col("product_category_id").alias("category_id"),
        "product_price",
        "product_status"
    )
    .dropDuplicates(["product_id"])
)


print("Product dimension rows:", dim_product.count())


dim_product.write.jdbc(
    JDBC_URL,
    "dim_product",
    mode="overwrite",
    properties=CONNECTION_PROPERTIES
)


print("dim_product loaded successfully")


spark.stop()