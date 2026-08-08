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
from pyspark.sql.functions import monotonically_increasing_id, col
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildRegionDimension")
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


dim_region = (
    stg.select(
        "market",
        col("order_region").alias("region")
    )
    .dropDuplicates()
)


dim_region = (
    dim_region
    .withColumn(
        "region_id",
        monotonically_increasing_id() + 1
    )
    .select(
        "region_id",
        "market",
        "region"
    )
)


print("Region dimension rows:", dim_region.count())


dim_region.write \
    .option("truncate", "true") \
    .jdbc(
        JDBC_URL,
        "dim_region",
        mode="overwrite",
        properties=CONNECTION_PROPERTIES
    )

print("dim_region loaded successfully")


spark.stop()