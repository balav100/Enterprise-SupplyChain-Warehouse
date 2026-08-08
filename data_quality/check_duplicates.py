import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Configure Hadoop home directory for Windows compatibility
hadoop_dir = os.path.abspath(os.path.join(project_root, "hadoop"))
os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["hadoop.home.dir"] = hadoop_dir
os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")


from database.config import CONNECTION_PROPERTIES, JDBC_URL
from pyspark.sql import SparkSession
from pyspark.sql.functions import count


spark = (
    SparkSession.builder
    .appName("DuplicateValidation")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


checks = {
    "dim_customer": "customer_id",
    "dim_product": "product_id",
    "dim_category": "category_id",
    "dim_shipping": "shipping_id",
    "dim_region": "region_id"
}


print("\nDUPLICATE KEY CHECK")
print("-" * 40)


for table, key in checks.items():

    df = spark.read.jdbc(
        JDBC_URL,
        table,
        properties=CONNECTION_PROPERTIES
    )

    duplicates = (
        df
        .groupBy(key)
        .agg(count("*").alias("count"))
        .filter("count > 1")
        .count()
    )

    print(
        f"{table}.{key}: {duplicates} duplicate keys"
    )


spark.stop()