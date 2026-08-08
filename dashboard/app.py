import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

from database.config import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)


st.set_page_config(
    page_title="Enterprise Supply Chain Analytics",
    layout="wide"
)


def get_connection():

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def load_data(query):

    conn = get_connection()

    df = pd.read_sql(
        query,
        conn
    )

    conn.close()

    return df



st.title(
    "🚚 Enterprise Supply Chain Analytics Dashboard"
)


st.divider()


sales = load_data(
"""
SELECT *
FROM analytics.sales_summary
"""
)


customer = load_data(
"""
SELECT *
FROM analytics.customer_analysis
"""
)


shipping = load_data(
"""
SELECT *
FROM analytics.shipping_performance
"""
)


region = load_data(
"""
SELECT *
FROM analytics.region_performance
"""
)



# KPI Section

total_sales = sales["total_sales"].sum()

total_profit = sales["total_profit"].sum()

total_orders = sales["total_orders"].sum()


col1, col2, col3 = st.columns(3)


col1.metric(
    "Total Revenue",
    f"${total_sales:,.2f}"
)


col2.metric(
    "Total Profit",
    f"${total_profit:,.2f}"
)


col3.metric(
    "Total Orders",
    f"{total_orders:,}"
)



st.divider()



st.subheader(
    "Monthly Revenue Trend"
)


fig = px.line(
    sales,
    x="month",
    y="total_sales",
    markers=True
)


st.plotly_chart(
    fig,
    use_container_width=True
)



st.subheader(
    "Customer Segment Performance"
)


fig2 = px.bar(
    customer,
    x="segment",
    y="revenue",
    text="revenue"
)


st.plotly_chart(
    fig2,
    use_container_width=True
)



st.subheader(
    "Regional Revenue"
)


fig3 = px.bar(
    region.head(10),
    x="region",
    y="revenue"
)


st.plotly_chart(
    fig3,
    use_container_width=True
)



st.subheader(
    "Shipping Performance"
)


st.dataframe(
    shipping,
    use_container_width=True
)