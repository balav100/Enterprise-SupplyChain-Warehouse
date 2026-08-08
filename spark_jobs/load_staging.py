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
from database.config import (
    DATASET_PATH,
    JDBC_URL,
    CONNECTION_PROPERTIES
)


spark = (
    SparkSession.builder
    .appName("LoadStaging")
    .config(
        "spark.jars",
        "/opt/airflow/jars/postgresql-42.7.3.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")



df = spark.read.csv(
    DATASET_PATH,
    header=True,
    inferSchema=True
)

print(f"Rows Read: {df.count()}")
print(f"Columns Read: {len(df.columns)}")



column_mapping = {
    "Type": "type",
    "Days for shipping (real)": "days_for_shipping_real",
    "Days for shipment (scheduled)": "days_for_shipment_scheduled",
    "Benefit per order": "benefit_per_order",
    "Sales per customer": "sales_per_customer",
    "Delivery Status": "delivery_status",
    "Late_delivery_risk": "late_delivery_risk",
    "Category Id": "category_id",
    "Category Name": "category_name",
    "Customer City": "customer_city",
    "Customer Country": "customer_country",
    "Customer Email": "customer_email",
    "Customer Fname": "customer_fname",
    "Customer Id": "customer_id",
    "Customer Lname": "customer_lname",
    "Customer Password": "customer_password",
    "Customer Segment": "customer_segment",
    "Customer State": "customer_state",
    "Customer Street": "customer_street",
    "Customer Zipcode": "customer_zipcode",
    "Department Id": "department_id",
    "Department Name": "department_name",
    "Latitude": "latitude",
    "Longitude": "longitude",
    "Market": "market",
    "Order City": "order_city",
    "Order Country": "order_country",
    "Order Customer Id": "order_customer_id",
    "order date (DateOrders)": "order_date",
    "Order Id": "order_id",
    "Order Item Cardprod Id": "order_item_cardprod_id",
    "Order Item Discount": "order_item_discount",
    "Order Item Discount Rate": "order_item_discount_rate",
    "Order Item Id": "order_item_id",
    "Order Item Product Price": "order_item_product_price",
    "Order Item Profit Ratio": "order_item_profit_ratio",
    "Order Item Quantity": "order_item_quantity",
    "Sales": "sales",
    "Order Item Total": "order_item_total",
    "Order Profit Per Order": "order_profit_per_order",
    "Order Region": "order_region",
    "Order State": "order_state",
    "Order Status": "order_status",
    "Order Zipcode": "order_zipcode",
    "Product Card Id": "product_card_id",
    "Product Category Id": "product_category_id",
    "Product Description": "product_description",
    "Product Image": "product_image",
    "Product Name": "product_name",
    "Product Price": "product_price",
    "Product Status": "product_status",
    "shipping date (DateOrders)": "shipping_date",
    "Shipping Mode": "shipping_mode"
}

for old_name, new_name in column_mapping.items():
    df = df.withColumnRenamed(old_name, new_name)



print("Loading stg_supply_chain...")

df.write.jdbc(
    url=JDBC_URL,
    table="stg_supply_chain",
    mode="overwrite",
    properties=CONNECTION_PROPERTIES
)

print("stg_supply_chain loaded successfully")

spark.stop()