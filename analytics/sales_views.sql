CREATE OR REPLACE VIEW analytics.sales_summary AS

SELECT

    DATE_TRUNC('month', order_date) AS month,

    SUM(sales) AS total_sales,

    SUM(profit) AS total_profit,

    SUM(quantity) AS total_quantity,

    COUNT(DISTINCT order_id) AS total_orders


FROM fact_orders

GROUP BY
    DATE_TRUNC('month', order_date)

ORDER BY month;