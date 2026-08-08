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
from database.config import JDBC_URL, CONNECTION_PROPERTIES


spark = (
    SparkSession.builder
    .appName("BuildCategoryDimension")
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


dim_category = (
    stg.select(
        "category_id",
        "category_name",
        "department_id",
        "department_name"
    )
    .dropDuplicates(["category_id"])
)


print("Category dimension rows:", dim_category.count())


dim_category.write.jdbc(
    JDBC_URL,
    "dim_category",
    mode="overwrite",
    properties=CONNECTION_PROPERTIES
)


print("dim_category loaded successfully")


spark.stop()