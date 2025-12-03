SELECT logged_in_user();

CREATE DATABASE IF NOT EXISTS sales_products;
USE sales_products;

CREATE EXTERNAL TABLE IF NOT EXISTS products (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    supplier STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir4}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS sales_summary (
    product_id STRING,
    payment_type STRING,
    total_quantity INT,
    avg_unit_price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${input_dir3}';

DROP TABLE IF EXISTS category_summary;

CREATE EXTERNAL TABLE IF NOT EXISTS category_summary (
    category STRING,
    total_quantity INT,
    avg_unit_price DOUBLE,
    brands_ranking ARRAY<STRUCT<brand: STRING, rank_in_category: INT>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

WITH product_agg AS (
    SELECT
        s.product_id,
        p.category,
        s.total_quantity,
        s.avg_unit_price,
        p.brand
    FROM sales_summary s
    JOIN products p ON s.product_id = p.product_id
),

category_totals AS (
    SELECT
        category,
        SUM(total_quantity) AS total_quantity,
        SUM(total_quantity * avg_unit_price) / SUM(total_quantity) AS avg_unit_price
    FROM product_agg
    GROUP BY category
),

brand_rank AS (
    SELECT
        category,
        brand,
        SUM(total_quantity) AS brand_quantity,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(total_quantity) DESC) AS rank_in_category
    FROM product_agg
    GROUP BY category, brand
),

brands_agg AS (
    SELECT
        category,
        collect_list(named_struct('brand', brand, 'rank_in_category', rank_in_category)) AS brands_ranking
    FROM brand_rank
    GROUP BY category
)

INSERT OVERWRITE TABLE category_summary
SELECT
    ct.category,
    ct.total_quantity,
    ct.avg_unit_price,
    ba.brands_ranking
FROM category_totals ct
JOIN brands_agg ba ON ct.category = ba.category;
