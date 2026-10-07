-- 01_models.sql : curated analytics layer on top of raw_* tables (dataset: grocery)
-- Run: bq query --use_legacy_sql=false < sql/01_models.sql

-- ---------- Dimensions ----------
CREATE OR REPLACE VIEW grocery.dim_product AS
SELECT product_id, product_name, category, cost_price, selling_price,
       selling_price - cost_price AS unit_margin,
       SAFE_DIVIDE(selling_price - cost_price, selling_price) AS margin_pct,
       supplier, shelf_life_days
FROM grocery.raw_products;

CREATE OR REPLACE VIEW grocery.dim_customer AS
SELECT * FROM grocery.raw_customers;

-- Festival calendar comes from festivals.csv (festival,start_date,end_date): you maintain it yearly.
-- A window = the days of elevated demand, including the pre-festival run-up.
CREATE OR REPLACE VIEW grocery.dim_festival AS
SELECT festival, start_date, end_date FROM grocery.raw_festivals;

-- ---------- Fact: one row per line item with revenue / cost / profit ----------
CREATE OR REPLACE TABLE grocery.fact_sales
PARTITION BY sale_date
CLUSTER BY product_id, customer_id AS
SELECT
  t.transaction_id, t.order_id, t.customer_id, t.product_id,
  DATE(t.transaction_ts)                         AS sale_date,
  EXTRACT(HOUR FROM t.transaction_ts)            AS sale_hour,
  EXTRACT(DAYOFWEEK FROM t.transaction_ts)       AS day_of_week,
  t.quantity, t.unit_price, t.discount_pct, t.payment_mode,
  t.quantity * t.unit_price * (1 - t.discount_pct / 100)       AS revenue,
  t.quantity * p.cost_price                                    AS cogs,
  t.quantity * t.unit_price * (1 - t.discount_pct / 100)
    - t.quantity * p.cost_price                                AS gross_profit,
  t.quantity * t.unit_price * t.discount_pct / 100             AS discount_given,
  f.festival
FROM grocery.raw_transactions t
JOIN grocery.raw_products p USING (product_id)
LEFT JOIN grocery.dim_festival f
  ON DATE(t.transaction_ts) BETWEEN f.start_date AND f.end_date;

-- ---------- Business performance ----------
-- Monthly P&L: revenue, COGS, gross profit, operating expenses, net profit / LOSS
CREATE OR REPLACE VIEW grocery.v_monthly_pnl AS
WITH s AS (
  SELECT DATE_TRUNC(sale_date, MONTH) AS month,
         SUM(revenue) AS revenue, SUM(cogs) AS cogs, SUM(gross_profit) AS gross_profit,
         SUM(discount_given) AS discounts, COUNT(DISTINCT order_id) AS orders
  FROM grocery.fact_sales GROUP BY 1),
e AS (
  SELECT DATE_TRUNC(expense_date, MONTH) AS month, SUM(amount) AS opex
  FROM grocery.raw_expenses GROUP BY 1),
x AS (
  SELECT DATE_TRUNC(expense_date, MONTH) AS month, category, SUM(amount) AS amount
  FROM grocery.raw_expenses GROUP BY 1, 2)
SELECT s.month, s.orders, s.revenue, s.cogs, s.gross_profit, s.discounts,
       IFNULL(e.opex, 0) AS opex,
       s.gross_profit - IFNULL(e.opex, 0) AS net_profit,
       IF(s.gross_profit - IFNULL(e.opex, 0) < 0, 'LOSS', 'PROFIT') AS status,
       SAFE_DIVIDE(s.gross_profit, s.revenue) AS gross_margin_pct,
       SAFE_DIVIDE(s.gross_profit - IFNULL(e.opex, 0), s.revenue) AS net_margin_pct,
       SAFE_DIVIDE(s.revenue, s.orders) AS avg_order_value
FROM s LEFT JOIN e USING (month);

CREATE OR REPLACE VIEW grocery.v_expense_breakdown AS
SELECT DATE_TRUNC(expense_date, MONTH) AS month, category, SUM(amount) AS amount
FROM grocery.raw_expenses GROUP BY 1, 2;

-- Loss makers: products selling below cost or with negative margin after discounts
CREATE OR REPLACE VIEW grocery.v_product_profitability AS
SELECT p.product_id, p.product_name, p.category,
       SUM(f.quantity) AS units, SUM(f.revenue) AS revenue, SUM(f.gross_profit) AS gross_profit,
       SAFE_DIVIDE(SUM(f.gross_profit), SUM(f.revenue)) AS margin_pct,
       SUM(f.discount_given) AS discounts,
       RANK() OVER (ORDER BY SUM(f.gross_profit) DESC) AS profit_rank,
       IF(SUM(f.gross_profit) < 0, TRUE, FALSE) AS is_loss_making
FROM grocery.fact_sales f JOIN grocery.dim_product p USING (product_id)
GROUP BY 1, 2, 3;

-- ---------- Peak hours ----------
CREATE OR REPLACE VIEW grocery.v_hourly_traffic AS
SELECT sale_hour,
       COUNT(DISTINCT order_id) / COUNT(DISTINCT sale_date) AS avg_orders_per_day,
       SUM(revenue) / COUNT(DISTINCT sale_date)             AS avg_revenue_per_day,
       SUM(quantity) / COUNT(DISTINCT sale_date)            AS avg_units_per_day,
       PERCENT_RANK() OVER (ORDER BY SUM(revenue) / COUNT(DISTINCT sale_date)) AS intensity
FROM grocery.fact_sales GROUP BY sale_hour;

-- Hour x weekday heatmap, split festival vs normal days
CREATE OR REPLACE VIEW grocery.v_hour_weekday_heatmap AS
SELECT day_of_week, sale_hour, festival IS NOT NULL AS is_festival,
       COUNT(DISTINCT order_id) / COUNT(DISTINCT sale_date) AS avg_orders
FROM grocery.fact_sales GROUP BY 1, 2, 3;

-- Peak-hour product demand => what must be on the shelf at 5-8pm (top sellers per peak hour)
CREATE OR REPLACE VIEW grocery.v_peak_hour_top_products AS
WITH peak AS (SELECT sale_hour FROM grocery.v_hourly_traffic WHERE intensity >= 0.75),
d AS (
  SELECT f.sale_hour, f.product_id, SUM(f.quantity) / COUNT(DISTINCT f.sale_date) AS avg_units
  FROM grocery.fact_sales f JOIN peak USING (sale_hour) GROUP BY 1, 2)
SELECT * EXCEPT(rk) FROM (
  SELECT d.*, p.product_name, ROW_NUMBER() OVER (PARTITION BY sale_hour ORDER BY avg_units DESC) AS rk
  FROM d JOIN grocery.dim_product p USING (product_id)) WHERE rk <= 10;

-- ---------- Availability ----------
CREATE OR REPLACE VIEW grocery.v_stockout_summary AS
SELECT a.product_id, p.product_name, p.category,
       COUNTIF(a.stockout_flag) AS stockout_days,
       COUNT(*) AS days_observed,
       SAFE_DIVIDE(COUNTIF(a.stockout_flag), COUNT(*)) AS stockout_rate,
       COUNTIF(a.closing_stock < a.reorder_level) AS low_stock_days
FROM grocery.raw_availability a JOIN grocery.dim_product p USING (product_id)
GROUP BY 1, 2, 3;

-- ---------- Daily demand per product (feeds forecasting, uplift and clearance views) ----------
CREATE OR REPLACE TABLE grocery.daily_product_demand
PARTITION BY sale_date CLUSTER BY product_id AS
SELECT sale_date, product_id, SUM(quantity) AS units
FROM grocery.fact_sales GROUP BY 1, 2;
