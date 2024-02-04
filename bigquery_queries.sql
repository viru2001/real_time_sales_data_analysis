-- create tables where streaming data will be loaded

CREATE TABLE sales-analysis-413205.sales_data.inventory_updates (
  product_id STRING,
  timestamp TIMESTAMP,
  quantity_change INTEGER,
  store_id STRING
);


CREATE TABLE sales-analysis-413205.sales_data.sales_transactions (
  transaction_id STRING,
  product_id STRING,
  timestamp TIMESTAMP,
  quantity INT64,
  unit_price FLOAT64,
  store_id STRING
);

--- create predfined data tables

CREATE EXTERNAL TABLE sales-analysis-413205.sales_data.stores
OPTIONS (
  format = 'CSV',
  uris = ['gs://sales_data_03/stores/stores_data.csv']
);

CREATE EXTERNAL TABLE sales-analysis-413205.sales_data.products
OPTIONS (
  format = 'CSV',
  uris = ['gs://sales_data_03/products/products_data.csv']
);

---select data from tables
select * from sales-analysis-413205.sales_data.products;
select * from sales-analysis-413205.sales_data.products;

-- take count of streaming data tables
select  "sales_transactions" as a,count(*) from sales-analysis-413205.sales_data.sales_transactions union all
select "inventory_updates" as a,count(*) from sales-analysis-413205.sales_data.inventory_updates;



