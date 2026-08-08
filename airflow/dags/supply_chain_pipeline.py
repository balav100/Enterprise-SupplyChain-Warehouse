from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="supply_chain_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["supply-chain", "warehouse"],
) as dag:

    load_staging = BashOperator(
        task_id="load_staging",
        bash_command="python /opt/airflow/spark_jobs/load_staging.py",
    )

    customer_dim = BashOperator(
        task_id="build_customer_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_customer_dimension.py",
    )

    category_dim = BashOperator(
        task_id="build_category_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_category_dimension.py",
    )

    product_dim = BashOperator(
        task_id="build_product_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_product_dimension.py",
    )

    shipping_dim = BashOperator(
        task_id="build_shipping_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_shipping_dimension.py",
    )

    region_dim = BashOperator(
        task_id="build_region_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_region_dimension.py",
    )

    date_dim = BashOperator(
        task_id="build_date_dimension",
        bash_command="python /opt/airflow/spark_jobs/dimensions/build_date_dimension.py",
    )

    fact_orders = BashOperator(
        task_id="build_fact_orders",
        bash_command="python /opt/airflow/spark_jobs/build_fact_orders.py",
    )

    quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command="python /opt/airflow/data_quality/run_quality_checks.py",
    )

    (
        load_staging
        >> customer_dim
        >> category_dim
        >> product_dim
        >> shipping_dim
        >> region_dim
        >> date_dim
        >> fact_orders
        >> quality_checks
    )