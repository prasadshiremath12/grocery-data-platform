-- 02b_forecast_and_plan.sql : daily. Forecast 45 days, compute festival uplift, build the inventory plan.
-- Needs: grocery.demand_forecast_model (02a), grocery.daily_product_demand (01).

-- 45-day forecast with 80% interval (use upper bound for safety stock)
CREATE OR REPLACE TABLE grocery.demand_forecast AS
SELECT product_id, DATE(forecast_timestamp) AS forecast_date,
       GREATEST(forecast_value, 0)        AS forecast_units,
       GREATEST(prediction_interval_lower_bound, 0) AS lower_units,
       GREATEST(prediction_interval_upper_bound, 0) AS upper_units
FROM ML.FORECAST(MODEL grocery.demand_forecast_model,
                 STRUCT(45 AS horizon, 0.8 AS confidence_level));

-- Historical festival uplift per category: avg daily units in festival window vs. 30 days before it
CREATE OR REPLACE VIEW grocery.v_festival_uplift AS
WITH fest AS (
  SELECT d.festival, d.start_date, d.end_date, p.category, p.product_id,
         SUM(IF(s.sale_date BETWEEN d.start_date AND d.end_date, s.units, 0))
           / (DATE_DIFF(d.end_date, d.start_date, DAY) + 1) AS festival_daily,
         SUM(IF(s.sale_date BETWEEN DATE_SUB(d.start_date, INTERVAL 40 DAY) AND DATE_SUB(d.start_date, INTERVAL 11 DAY), s.units, 0))
           / 30 AS baseline_daily
  FROM grocery.dim_festival d
  CROSS JOIN grocery.dim_product p
  JOIN grocery.daily_product_demand s ON s.product_id = p.product_id
   AND s.sale_date BETWEEN DATE_SUB(d.start_date, INTERVAL 40 DAY) AND d.end_date
  GROUP BY 1, 2, 3, 4, 5)
SELECT festival, product_id, category,
       AVG(SAFE_DIVIDE(festival_daily, baseline_daily)) AS uplift_factor
FROM fest WHERE baseline_daily > 0 GROUP BY 1, 2, 3;

-- Inventory plan for the next upcoming festival window (+ 7 day buffer)
-- order_qty = forecast over horizon * festival uplift (if the forecast does not already include it) + safety stock - current stock
CREATE OR REPLACE TABLE grocery.inventory_plan AS
WITH nxt AS (
  SELECT festival, start_date, end_date FROM grocery.dim_festival
  WHERE end_date >= CURRENT_DATE() ORDER BY start_date LIMIT 1),
stock AS (
  SELECT product_id, closing_stock AS current_stock, reorder_level
  FROM grocery.raw_availability
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) = 1),
need AS (
  SELECT f.product_id, n.festival, n.start_date, n.end_date,
         SUM(f.forecast_units) AS base_forecast,
         SUM(f.upper_units)    AS upper_forecast
  FROM grocery.demand_forecast f CROSS JOIN nxt n
  WHERE f.forecast_date BETWEEN DATE_SUB(n.start_date, INTERVAL 7 DAY) AND n.end_date
  GROUP BY 1, 2, 3, 4)
SELECT n.festival, n.start_date, n.end_date, n.product_id, p.product_name, p.category, p.supplier,
       p.shelf_life_days,
       s.current_stock, s.reorder_level,
       ROUND(n.base_forecast) AS base_forecast_units,
       ROUND(IFNULL(u.uplift_factor, 1), 2) AS historical_uplift,
       ROUND(n.upper_forecast * GREATEST(IFNULL(u.uplift_factor, 1) / 2, 1)) AS target_stock,
       GREATEST(CAST(ROUND(n.upper_forecast * GREATEST(IFNULL(u.uplift_factor, 1) / 2, 1) - s.current_stock) AS INT64), 0) AS suggested_order_qty,
       -- perishables: cap order to what can sell inside shelf life
       IF(p.shelf_life_days <= 7, TRUE, FALSE) AS perishable
FROM need n
JOIN grocery.dim_product p USING (product_id)
LEFT JOIN stock s USING (product_id)
LEFT JOIN grocery.v_festival_uplift u ON u.product_id = n.product_id AND u.festival = n.festival;

-- Peak-hour shelf restock list: avg units sold per hour for top hours vs. morning stock
CREATE OR REPLACE VIEW grocery.v_restock_alerts AS
SELECT i.product_id, i.product_name, i.current_stock, i.reorder_level,
       'BELOW_REORDER_LEVEL' AS alert
FROM grocery.inventory_plan i WHERE i.current_stock < i.reorder_level;

-- Forecast accuracy check (run after a few weeks): MAPE per product on the last 30 days
-- SELECT * FROM ML.EVALUATE(MODEL grocery.demand_forecast_model);
