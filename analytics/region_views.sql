CREATE OR REPLACE VIEW analytics.region_performance AS

SELECT

    r.market,

    r.region,

    SUM(f.sales) AS revenue,

    SUM(f.profit) AS profit,

    COUNT(DISTINCT f.order_id) AS orders


FROM fact_orders f

JOIN dim_region r

ON f.region_id = r.region_id


GROUP BY

r.market,
r.region

ORDER BY revenue DESC;