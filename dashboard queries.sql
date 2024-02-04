-- get units_sold and sales revenue trends hourly ,daily and weekly
select  TIMESTAMP_TRUNC(timestamp, HOUR)  AS hour_timestamp,sum(quantity) as units_sold,sum(quantity*unit_price) as sales_revenue from sales-analysis-413205.sales_data.sales_transactions
group by hour_timestamp;

select  TIMESTAMP_TRUNC(timestamp, DAY)  AS day_timestamp,sum(quantity) as units_sold,sum(quantity*unit_price) as sales_revenue from sales-analysis-413205.sales_data.sales_transactions
group by day_timestamp;

select  TIMESTAMP_TRUNC(timestamp, WEEK)  AS week_timestamp,sum(quantity) as units_sold,sum(quantity*unit_price) as sales_revenue from sales-analysis-413205.sales_data.sales_transactions
group by week_timestamp;

-- Product Wise Units sold and sales revenue data
select
    p.name as product_name,
    sum(st.quantity) as units_sold,
    sum(st.quantity*st.unit_price) as sales_revenue
from
    sales-analysis-413205.sales_data.sales_transactions as st
inner join
    sales-analysis-413205.sales_data.products as p 
on 
	st.product_id = p.product_id
group by
    p.name;

-- real-time inventory levels and identify products that are at risk of stockouts

WITH ProductSalesPerDay AS (
  SELECT
    iu.product_id,
    TIMESTAMP_TRUNC(iu.timestamp, DAY) AS day_timestamp,
    SUM(CASE WHEN iu.quantity_change < 0 THEN iu.quantity_change*(-1) END) AS units_sold_in_day
  FROM
    sales-analysis-413205.sales_data.inventory_updates AS iu
  GROUP BY
    iu.product_id,
    day_timestamp
),
x AS (
SELECT
  p.product_id as product__id,
  SUM(iu.quantity_change) AS current_stock,
FROM
  sales-analysis-413205.sales_data.inventory_updates AS iu
INNER JOIN
  sales-analysis-413205.sales_data.products AS p ON iu.product_id = p.product_id
group by product__id),
y as(
select 
 ProductSalesPerDay.product_id as pid,
 avg(units_sold_in_day) as avg_units_sold_in_day
from ProductSalesPerDay
group by product_id)
select x.product__id,x.current_stock, y.avg_units_sold_in_day
from x
inner join y
on x.product__id = y.pid;

-- location wise sales revenue and units sold

select
    s.location as store_location,	
	concat(s.latitude,',',s.longitude) as geo_location,
    sum(st.quantity) as units_sold,
    sum(st.quantity*st.unit_price) as sales_revenue
from
    sales-analysis-413205.sales_data.sales_transactions as st
inner join
    sales-analysis-413205.sales_data.stores as s on st.store_id = s.store_id
group by
    store_location, geo_location;

-- sales through rate

select
    p.product_id as product_id,
    SUM(CASE WHEN iu.quantity_change < 0 THEN iu.quantity_change*(-1) END)/SUM(CASE WHEN iu.quantity_change > 0 THEN iu.quantity_change END) as sell_through_rate,
from
    sales-analysis-413205.sales_data.inventory_updates as iu
inner join
    sales-analysis-413205.sales_data.products as p on iu.product_id = p.product_id
group by
    p.product_id;

-- category wise total revenue and units sold
select
    p.category as product_category,
    sum(st.quantity) as units_sold,
    sum(st.quantity*st.unit_price) as sales_revenue
from
    sales-analysis-413205.sales_data.sales_transactions as st
inner join
    sales-analysis-413205.sales_data.products as p on st.product_id = p.product_id
group by
    product_category;
