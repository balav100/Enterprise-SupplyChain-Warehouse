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
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("NullValidation")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


checks = {
    "fact_orders": [
        "order_id",
        "customer_id",
        "product_id",
        "category_id",
        "shipping_id",
        "region_id",
        "order_date"
    ],

    "dim_customer": [
        "customer_id"
    ],

    "dim_product": [
        "product_id"
    ],

    "dim_category": [
        "category_id"
    ],

    "dim_shipping": [
        "shipping_id"
    ],

    "dim_region": [
        "region_id"
    ]
}


print("\nNULL CHECK")
print("-" * 40)


for table, columns in checks.items():

    df = spark.read.jdbc(
        JDBC_URL,
        table,
        properties=CONNECTION_PROPERTIES
    )

    for column in columns:

        null_count = (
            df
            .filter(col(column).isNull())
            .count()
        )

        print(
            f"{table}.{column}: {null_count}"
        )


spark.stop()