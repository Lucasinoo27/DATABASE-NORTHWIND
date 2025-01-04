-- Vytvorenie databázy a použitie schémy
CREATE DATABASE IF NOT EXISTS OTTER_NORTHWIND;
USE DATABASE OTTER_NORTHWIND;
CREATE SCHEMA IF NOT EXISTS OTTER_NORTHWIND.staging;
USE SCHEMA OTTER_NORTHWIND.staging;

-- Vytvorenie staging tabuliek
CREATE OR REPLACE TABLE categories_staging
(      
    CategoryID INTEGER,
    CategoryName VARCHAR(25),
    Description VARCHAR(255)
);

CREATE OR REPLACE TABLE customers_staging
(      
    CustomerID INTEGER,
    CustomerName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15)
);

CREATE OR REPLACE TABLE employees_staging
(
    EmployeeID INTEGER,
    LastName VARCHAR(15),
    FirstName VARCHAR(15),
    BirthDate TIMESTAMP_NTZ, -- Snowflake preferuje TIMESTAMP_NTZ
    Photo VARCHAR(25),
    Notes VARCHAR(1024)
);

CREATE OR REPLACE TABLE shippers_staging
(
    ShipperID INTEGER,
    ShipperName VARCHAR(25),
    Phone VARCHAR(15)
);

CREATE OR REPLACE TABLE suppliers_staging
(
    SupplierID INTEGER,
    SupplierName VARCHAR(50),
    ContactName VARCHAR(50),
    Address VARCHAR(50),
    City VARCHAR(20),
    PostalCode VARCHAR(10),
    Country VARCHAR(15),
    Phone VARCHAR(15)
);

CREATE OR REPLACE TABLE products_staging
(
    ProductID INTEGER,
    ProductName VARCHAR(50),
    SupplierID INTEGER,
    CategoryID INTEGER,
    Unit VARCHAR(25),
    Price NUMBER
);

CREATE OR REPLACE TABLE orders_staging
(
    OrderID INTEGER,
    CustomerID INTEGER,
    EmployeeID INTEGER,
    OrderDate TIMESTAMP_NTZ,
    ShipperID INTEGER
);

CREATE OR REPLACE TABLE orderdetails_staging
(
    OrderDetailID INTEGER,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER
);

-- Vytvorenie stage
CREATE OR REPLACE STAGE OTTER_NORTHWIND_stage;

-- Načítanie CSV súborov do staging tabuliek
COPY INTO categories_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_categories.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO customers_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_customers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO employees_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_employees.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO shippers_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_shippers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO suppliers_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_suppliers.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO products_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_products.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orders_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_orders.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

COPY INTO orderdetails_staging
FROM @OTTER_NORTHWIND_stage/northwind_table_orderdetails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- DIM_TABUĽKY
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('DAY', OrderDate)) AS dim_dateID,
    DATE_TRUNC('DAY', OrderDate) AS date,
    EXTRACT(YEAR FROM OrderDate) AS year,
    EXTRACT(MONTH FROM OrderDate) AS month,
    EXTRACT(DAY FROM OrderDate) AS day,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM OrderDate) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM orders_staging;

CREATE OR REPLACE TABLE DIM_SUPPLIERS AS
SELECT DISTINCT
    SupplierID AS dim_supplierID,
    SupplierName AS supplier_name,
    ContactName AS contact_name,
    City AS city,
    Country AS country,
    Phone AS phone
FROM suppliers_staging;

CREATE OR REPLACE TABLE DIM_PRODUCTS AS
SELECT DISTINCT
    ProductID AS dim_productID,
    ProductName AS product_name,
    CategoryID AS category_id,
    Unit AS unit,
    Price AS price
FROM products_staging;

CREATE OR REPLACE TABLE DIM_CUSTOMERS AS
SELECT DISTINCT
    CustomerID AS dim_customerID,
    CustomerName AS customer_name,
    ContactName AS contact_name,
    City AS city,
    Country AS country,
    PostalCode AS postal_code
FROM customers_staging;

CREATE OR REPLACE TABLE DIM_EMPLOYEES AS
SELECT DISTINCT
    EmployeeID AS dim_employeeID,
    FirstName AS first_name,
    LastName AS last_name,
    BirthDate AS birth_date,
    Photo AS photo,
    Notes AS notes
FROM employees_staging;

CREATE OR REPLACE TABLE DIM_SHIPPERS AS
SELECT DISTINCT
    ShipperID AS dim_shipperID,
    ShipperName AS shipper_name,
    Phone AS phone
FROM shippers_staging;

CREATE OR REPLACE TABLE DIM_CATEGORIES AS
SELECT DISTINCT
    CategoryID AS dim_categoryID,
    CategoryName AS category_name,
    Description AS description
FROM categories_staging;

-- FACT_TABUĽKA
CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT
    o.OrderID AS fact_orderID,
    o.OrderDate AS order_date,
    d.dim_dateID AS date_id,
    c.dim_customerID AS customer_id,
    e.dim_employeeID AS employee_id,
    s.dim_supplierID AS supplier_id,
    sh.dim_shipperID AS shipper_id,
    p.dim_productID AS product_id,
    p.category_id AS dim_categoryID,
    od.Quantity AS quantity,
    p.price AS unit_price,
    od.Quantity * p.price AS total_price
FROM orders_staging o
JOIN orderdetails_staging od ON o.OrderID = od.OrderID
JOIN DIM_DATE d ON DATE_TRUNC('DAY', o.OrderDate) = d.date
JOIN DIM_CUSTOMERS c ON o.CustomerID = c.dim_customerID
JOIN DIM_PRODUCTS p ON od.ProductID = p.dim_productID
JOIN DIM_SUPPLIERS s ON p.category_id = s.dim_supplierID
JOIN DIM_EMPLOYEES e ON o.EmployeeID = e.dim_employeeID
JOIN DIM_SHIPPERS sh ON o.ShipperID = sh.dim_shipperID;

-- DROP staging tabuliek
DROP TABLE IF EXISTS categories_staging;
DROP TABLE IF EXISTS customers_staging;
DROP TABLE IF EXISTS employees_staging;
DROP TABLE IF EXISTS shippers_staging;
DROP TABLE IF EXISTS suppliers_staging;
DROP TABLE IF EXISTS products_staging;
DROP TABLE IF EXISTS orders_staging;
DROP TABLE IF EXISTS orderdetails_staging;