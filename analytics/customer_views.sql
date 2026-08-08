CREATE OR REPLACE VIEW analytics.customer_analysis AS

SELECT

    c.segment,

    COUNT(DISTINCT f.customer_id) AS customers,

    SUM(f.sales) AS revenue,

    SUM(f.profit) AS profit


FROM fact_orders f

JOIN dim_customer c

ON f.customer_id = c.customer_id


GROUP BY
    c.segment

ORDER BY revenue DESC;