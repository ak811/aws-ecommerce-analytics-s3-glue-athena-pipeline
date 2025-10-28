-- ITCS 6190/8190 Hands-on L11: Athena Queries
USE "handson11-output-db";

-- Q1: Cumulative Sales Over Time (Year = 2022) — LIMIT 10
WITH base AS (
  SELECT COALESCE(try(date_parse("Date", '%m/%d/%Y')), try(date_parse("Date", '%m-%d-%y')), try(date_parse("Date", '%m/%d/%y'))) AS order_dt,
         try_cast("Amount" AS double) AS amount
  FROM "raw"
)
SELECT order_dt,
       SUM(amount) OVER (ORDER BY order_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sales
FROM base
WHERE order_dt IS NOT NULL AND year(order_dt)=2022
ORDER BY order_dt
LIMIT 10;

-- Q2: Geographic "Hotspot" for Unprofitable Products (proxy: cancelled/returned/refund) — LIMIT 10
SELECT "ship-state" AS state,
       SUM(-1 * try_cast("Amount" AS double)) AS total_negative_revenue
FROM "raw"
WHERE lower("Status") IN ('cancelled','returned','refund')
  AND "ship-state" IS NOT NULL
GROUP BY "ship-state"
ORDER BY total_negative_revenue ASC
LIMIT 10;

-- Q3: Impact of Discounts on Profitability by Sub-Category (proxy: promotion present) — LIMIT 10
SELECT "Category" AS sub_category,
       CASE WHEN COALESCE(NULLIF(TRIM("promotion-ids"), ''), '') <> '' THEN 'Promo' ELSE 'No Promo' END AS discount_flag,
       SUM(try_cast("Amount" AS double)) AS total_sales,
       SUM(try_cast("Qty" AS integer))  AS total_units,
       CASE WHEN SUM(try_cast("Qty" AS integer)) > 0
            THEN SUM(try_cast("Amount" AS double)) / SUM(try_cast("Qty" AS integer))
            ELSE NULL END AS sales_per_unit_proxy
FROM "raw"
GROUP BY "Category",
         CASE WHEN COALESCE(NULLIF(TRIM("promotion-ids"), ''), '') <> '' THEN 'Promo' ELSE 'No Promo' END
ORDER BY sub_category, discount_flag
LIMIT 10;

-- Q4: Top 3 Most Profitable Products Within Each Category (proxy: rank by sales) — LIMIT 10
WITH product_totals AS (
  SELECT "Category" AS category,
         "SKU"      AS sku,
         SUM(try_cast("Amount" AS double)) AS total_sales
  FROM "raw"
  GROUP BY "Category","SKU"
),
ranked AS (
  SELECT category, sku, total_sales,
         RANK() OVER (PARTITION BY category ORDER BY total_sales DESC) AS rnk
  FROM product_totals
)
SELECT category, sku, total_sales, rnk
FROM ranked
WHERE rnk <= 3
ORDER BY category, rnk, total_sales DESC
LIMIT 10;

-- Q5: Monthly Sales and Profit Growth (profit proxy = sales) — LIMIT 10
WITH base AS (
  SELECT date_trunc('month', COALESCE(try(date_parse("Date", '%m/%d/%Y')), try(date_parse("Date", '%m-%d-%y')), try(date_parse("Date", '%m/%d/%y')))) AS month,
         try_cast("Amount" AS double) AS amount
  FROM "raw"
),
monthly AS (
  SELECT month,
         SUM(amount) AS total_sales,
         SUM(amount) AS profit_proxy
  FROM base
  WHERE month IS NOT NULL
  GROUP BY 1
)
SELECT month,
       total_sales,
       profit_proxy,
       (total_sales - LAG(total_sales) OVER (ORDER BY month)) / NULLIF(LAG(total_sales) OVER (ORDER BY month),0) AS sales_growth_mom,
       (profit_proxy - LAG(profit_proxy) OVER (ORDER BY month)) / NULLIF(LAG(profit_proxy) OVER (ORDER BY month),0) AS profit_growth_mom
FROM monthly
ORDER BY month
LIMIT 10;
