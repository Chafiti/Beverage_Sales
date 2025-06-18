CREATE TABLE liquor_sales_2023 (
  invoice_line_no VARCHAR(50),
  date TEXT,
  store_num INT,
  store_name VARCHAR(255),
  city VARCHAR(100),
  store_location POINT,
  county VARCHAR(100),
  category_name VARCHAR(100),
  vendor_name VARCHAR(255),
  item_number VARCHAR(20),
  item_desc TEXT,
  pack INT,
  bottle_volume_ml INT,
  state_bottle_cost DECIMAL(10,2),
  state_bottle_retail DECIMAL(10,2),
  bottles_sold INT,
  sale_dollars DECIMAL(12,2),
  sale_liters DECIMAL(12,4),
  sale_gallons DECIMAL(12,4)
);

LOAD DATA LOCAL INFILE 'C:/Users/Admin/Downloads/Iowa_Liquor_Sales_2023.csv'
INTO TABLE liquor_sales_2023
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  invoice_line_no,
  date,
  store_num,
  store_name,
  city,
  @store_location,
  county,
  category_name,
  vendor_name,
  item_number,
  item_desc,
  pack,
  bottle_volume_ml,
  state_bottle_cost,
  state_bottle_retail,
  bottles_sold,
  sale_dollars,
  sale_liters,
  sale_gallons
)
SET store_location = IF(@store_location != '', ST_GeomFromText(@store_location), NULL);

CREATE TABLE liquor_sales_2024 (
  invoice_line_no VARCHAR(50),
  date TEXT,
  store_num INT,
  store_name VARCHAR(255),
  city VARCHAR(100),
  store_location POINT,
  county VARCHAR(100),
  category_name VARCHAR(100),
  vendor_name VARCHAR(255),
  item_number VARCHAR(20),
  item_desc TEXT,
  pack INT,
  bottle_volume_ml INT,
  state_bottle_cost DECIMAL(10,2),
  state_bottle_retail DECIMAL(10,2),
  bottles_sold INT,
  sale_dollars DECIMAL(12,2),
  sale_liters DECIMAL(12,4),
  sale_gallons DECIMAL(12,4)
);


LOAD DATA LOCAL INFILE 'C:/Users/Admin/Downloads/Iowa_Liquor_Sales_2024.csv'
INTO TABLE liquor_sales_2024
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  invoice_line_no,
  date,
  store_num,
  store_name,
  city,
  @store_location,
  county,
  category_name,
  vendor_name,
  item_number,
  item_desc,
  pack,
  bottle_volume_ml,
  state_bottle_cost,
  state_bottle_retail,
  bottles_sold,
  sale_dollars,
  sale_liters,
  sale_gallons
)
SET store_location = IF(@store_location != '', ST_GeomFromText(@store_location), NULL);



-- Normalizing date text to promote data type to date
UPDATE liquor_sales_2024
SET date = STR_TO_DATE(date, '%m/%d/%Y');


SELECT
  store_name,
  ST_AsText(store_location) AS location
FROM liquor_sales_2023
LIMIT 10;

SELECT
  DATE_FORMAT(date, '%Y-%m') AS month,
  SUM(sale_dollars) AS total_sales
FROM (
  SELECT date, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT date, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY month
ORDER BY month;

-- Margin Analysis
SELECT
  category_name,
  ROUND(AVG(state_bottle_retail - state_bottle_cost), 2) AS avg_margin_per_bottle,
  ROUND(AVG((state_bottle_retail - state_bottle_cost) / NULLIF(state_bottle_cost, 0)) * 100, 2) AS avg_margin_percent
FROM liquor_sales_2024
GROUP BY category_name
ORDER BY avg_margin_per_bottle DESC;


-- Top Margin Categories
SELECT
  category_name,
  ROUND(AVG(state_bottle_retail - state_bottle_cost), 2) AS avg_margin_per_bottle,
  ROUND(AVG((state_bottle_retail - state_bottle_cost) / NULLIF(state_bottle_cost, 0)) * 100, 2) AS avg_margin_percent,
  SUM(sale_dollars) AS total_revenue
FROM (
  SELECT category_name, state_bottle_retail, state_bottle_cost, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT category_name, state_bottle_retail, state_bottle_cost, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY category_name
ORDER BY avg_margin_percent DESC
LIMIT 10;


-- Seasonal Trends by Category
SELECT
  DATE_FORMAT(date, '%Y-%m') AS month,
  category_name,
  SUM(sale_dollars) AS revenue
FROM (
  SELECT date, category_name, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT date, category_name, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY month, category_name
ORDER BY month, revenue DESC;


-- Regional Sales by Category
SELECT
  county,
  category_name,
  SUM(sale_dollars) AS category_revenue
FROM (
  SELECT county, category_name, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT county, category_name, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY county, category_name
ORDER BY county, category_revenue DESC;


-- High SKU Count Vendors With Low Market Share
SELECT
  vendor_name,
  COUNT(DISTINCT item_desc) AS sku_count,
  SUM(sale_dollars) AS total_sales
FROM (
  SELECT vendor_name, item_desc, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT vendor_name, item_desc, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY vendor_name
HAVING sku_count > 20 AND total_sales < 50000
ORDER BY sku_count DESC;

-- Bottle Size Preferences by Category
SELECT
  category_name,
  bottle_volume_ml,
  COUNT(*) AS orders,
  SUM(bottles_sold) AS bottles_sold
FROM (
  SELECT category_name, bottle_volume_ml, bottles_sold FROM liquor_sales_2023
  UNION ALL
  SELECT category_name, bottle_volume_ml, bottles_sold FROM liquor_sales_2024
) AS combined
GROUP BY category_name, bottle_volume_ml
ORDER BY category_name, bottles_sold DESC;

-- Vendor vs. Category Specialization
SELECT
  vendor_name,
  category_name,
  SUM(sale_dollars) AS revenue
FROM (
  SELECT vendor_name, category_name, sale_dollars FROM liquor_sales_2023
  UNION ALL
  SELECT vendor_name, category_name, sale_dollars FROM liquor_sales_2024
) AS combined
GROUP BY vendor_name, category_name
ORDER BY vendor_name, revenue DESC;

-- Top 10 vendors with most revenue
SELECT vendor_name, SUM(sale_dollars) AS revenue
FROM (
	SELECT vendor_name, sale_dollars FROM liquor_sales_2023
    UNION ALL
    SELECT vendor_name, sale_dollars FROM liquor_sales_2024
    ) as combined
    GROUP BY vendor_name
    ORDER BY revenue DESC
    LIMIT 10;
    
-- Top 10 biggest spenders
SELECT store_name, SUM(bottles_sold) AS total_bottles_purchased, SUM(sale_dollars) AS money_spent
FROM (
	SELECT store_name, bottles_sold, sale_dollars FROM liquor_sales_2023
    UNION ALL
    SELECT store_name, bottles_sold, sale_dollars FROM liquor_sales_2024
    ) as combined
    GROUP BY store_name
    ORDER BY money_spent DESC
    LIMIT 10;
    
