SELECT
    d.year,
    d.month,
    COUNT(f.fact_orderID) AS order_count
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.date_id = d.dim_dateID
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

SELECT
    p.product_name,
    SUM(f.quantity) AS total_quantity
FROM FACT_ORDERS f
JOIN DIM_PRODUCTS p ON f.product_id = p.dim_productID
GROUP BY p.product_name
ORDER BY total_quantity DESC
LIMIT 5;

SELECT
    c.country,
    COUNT(f.fact_orderID) AS order_count
FROM FACT_ORDERS f
JOIN DIM_CUSTOMERS c ON f.customer_id = c.CustomerID
GROUP BY c.country
ORDER BY order_count DESC;

SELECT
    d.year,
    SUM(f.total_price) AS total_revenue
FROM FACT_ORDERS f
JOIN DIM_DATE d ON f.date_id = d.dim_dateID
GROUP BY d.year
ORDER BY d.year;

SELECT
    s.supplier_name,
    SUM(f.total_price) AS supplier_revenue
FROM FACT_ORDERS f
JOIN DIM_SUPPLIERS s ON f.supplier_id = s.dim_supplierID
GROUP BY s.supplier_name
ORDER BY supplier_revenue DESC;
