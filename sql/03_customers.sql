-- 03_customers.sql : RFM segmentation, personalised recommendations, offers, new-customer acquisition

-- ---------- RFM ----------
CREATE OR REPLACE TABLE grocery.customer_rfm AS
WITH base AS (
  SELECT customer_id,
         DATE_DIFF(CURRENT_DATE(), MAX(sale_date), DAY) AS recency_days,
         COUNT(DISTINCT order_id) AS frequency,
         SUM(revenue) AS monetary,
         SUM(gross_profit) AS profit,
         MAX(sale_date) AS last_purchase
  FROM grocery.fact_sales GROUP BY 1),
scored AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r,   -- 5 = most recent
    NTILE(5) OVER (ORDER BY frequency)         AS f,
    NTILE(5) OVER (ORDER BY monetary)          AS m
  FROM base)
SELECT *,
  CASE
    WHEN r >= 4 AND f >= 4 AND m >= 4 THEN 'Champions'
    WHEN r >= 3 AND f >= 3             THEN 'Loyal'
    WHEN r >= 4 AND f <= 2             THEN 'New / Promising'
    WHEN r <= 2 AND f >= 4             THEN 'At Risk (was loyal)'
    WHEN r <= 2 AND f <= 2             THEN 'Lost / Dormant'
    ELSE 'Needs Attention'
  END AS segment
FROM scored;

-- ---------- Product affinity (market-basket: lift) ----------
CREATE OR REPLACE TABLE grocery.product_affinity AS
WITH orders AS (SELECT DISTINCT order_id, product_id FROM grocery.fact_sales),
n AS (SELECT COUNT(DISTINCT order_id) AS total FROM orders),
single AS (SELECT product_id, COUNT(*) AS cnt FROM orders GROUP BY 1),
pairs AS (
  SELECT a.product_id AS p1, b.product_id AS p2, COUNT(*) AS together
  FROM orders a JOIN orders b ON a.order_id = b.order_id AND a.product_id <> b.product_id
  GROUP BY 1, 2)
SELECT p1, p2, together,
       SAFE_DIVIDE(together, s1.cnt) AS confidence,
       SAFE_DIVIDE(together * n.total, s1.cnt * s2.cnt) AS lift
FROM pairs JOIN single s1 ON s1.product_id = p1 JOIN single s2 ON s2.product_id = p2 CROSS JOIN n
WHERE together >= 20;

-- ---------- Personalised recommendations (top 5 per customer) ----------
-- Score = habitual repurchase (items they buy regularly but not in last 14 days)
--       + affinity-based cross-sell + currently trending category (festival) boost
CREATE OR REPLACE TABLE grocery.customer_recommendations AS
WITH cust_items AS (
  SELECT customer_id, product_id, COUNT(DISTINCT order_id) AS buys, MAX(sale_date) AS last_bought
  FROM grocery.fact_sales WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  GROUP BY 1, 2),
repurchase AS (
  SELECT customer_id, product_id, 'Time to restock' AS reason,
         buys * 1.0 AS score
  FROM cust_items
  WHERE buys >= 3 AND last_bought < DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)),
crosssell AS (
  SELECT ci.customer_id, a.p2 AS product_id, 'Customers like you also buy' AS reason,
         SUM(a.lift * a.confidence * ci.buys) AS score
  FROM cust_items ci JOIN grocery.product_affinity a ON a.p1 = ci.product_id
  WHERE a.lift > 1.05
    AND NOT EXISTS (SELECT 1 FROM cust_items x WHERE x.customer_id = ci.customer_id AND x.product_id = a.p2)
  GROUP BY 1, 2),
festive AS (
  -- products with the strongest historical uplift for the next festival that starts within 21 days
  SELECT c.customer_id, u.product_id, CONCAT(u.festival, ' special') AS reason, 0.5 AS score
  FROM grocery.dim_customer c
  CROSS JOIN (
    SELECT u.festival, u.product_id,
           ROW_NUMBER() OVER (PARTITION BY u.festival ORDER BY u.uplift_factor DESC) AS rn
    FROM grocery.v_festival_uplift u
    JOIN (SELECT festival FROM grocery.dim_festival
          WHERE end_date >= CURRENT_DATE() AND start_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 21 DAY)) f
      USING (festival)
    WHERE u.uplift_factor > 1.2) u
  WHERE u.rn <= 8),
allc AS (SELECT * FROM repurchase UNION ALL SELECT * FROM crosssell UNION ALL SELECT * FROM festive),
ranked AS (
  SELECT customer_id, product_id, ARRAY_AGG(reason ORDER BY score DESC LIMIT 1)[OFFSET(0)] AS reason,
         SUM(score) AS score
  FROM allc GROUP BY 1, 2),
top5 AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY score DESC) AS rk FROM ranked)
SELECT t.customer_id, t.product_id, p.product_name, p.category, p.selling_price, t.reason, t.score, t.rk
FROM top5 t JOIN grocery.dim_product p USING (product_id)
-- never push items that are out of stock
LEFT JOIN (SELECT product_id, closing_stock FROM grocery.raw_availability
           QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) = 1) s USING (product_id)
WHERE t.rk <= 5 AND IFNULL(s.closing_stock, 1) > 0;

-- ---------- Discount offers per segment (margin-aware) ----------
-- Discount is capped so the product keeps >= 5% margin; excess-stock items get a bigger push.
CREATE OR REPLACE TABLE grocery.customer_offers AS
WITH seg_disc AS (
  SELECT * FROM UNNEST([
    STRUCT('Champions' AS segment, 5 AS pct, 'Thank-you reward' AS theme),
    ('Loyal', 7, 'Loyalty bonus'),
    ('New / Promising', 10, 'Welcome back'),
    ('Needs Attention', 10, 'We miss you'),
    ('At Risk (was loyal)', 15, 'Come back offer'),
    ('Lost / Dormant', 20, 'Win-back offer')]))
SELECT r.customer_id, r.product_id, r.product_name, r.selling_price, r.reason, r.rk,
       c.segment, d.theme,
       LEAST(d.pct, FLOOR(100 * SAFE_DIVIDE(p.selling_price - p.cost_price * 1.05, p.selling_price))) AS discount_pct,
       ROUND(r.selling_price * (1 - LEAST(d.pct,
         FLOOR(100 * SAFE_DIVIDE(p.selling_price - p.cost_price * 1.05, p.selling_price))) / 100), 2) AS offer_price
FROM grocery.customer_recommendations r
JOIN grocery.customer_rfm c USING (customer_id)
JOIN seg_disc d USING (segment)
JOIN grocery.dim_product p USING (product_id);

-- ---------- Growth: acquiring new customers ----------
-- 1) Which categories bring customers in on first purchase (use as welcome-offer hero items)
CREATE OR REPLACE VIEW grocery.v_acquisition_hooks AS
WITH first_orders AS (
  SELECT customer_id, ARRAY_AGG(order_id ORDER BY sale_date LIMIT 1)[OFFSET(0)] AS first_order
  FROM grocery.fact_sales GROUP BY 1),
f AS (
  SELECT s.customer_id, s.product_id FROM grocery.fact_sales s
  JOIN first_orders fo ON fo.first_order = s.order_id AND fo.customer_id = s.customer_id),
retained AS (
  SELECT customer_id FROM grocery.fact_sales GROUP BY 1 HAVING COUNT(DISTINCT order_id) >= 4)
SELECT p.product_name, p.category, COUNT(*) AS first_purchase_count,
       COUNTIF(r.customer_id IS NOT NULL) AS became_loyal,
       SAFE_DIVIDE(COUNTIF(r.customer_id IS NOT NULL), COUNT(*)) AS loyalty_conversion
FROM f JOIN grocery.dim_product p USING (product_id) LEFT JOIN retained r USING (customer_id)
GROUP BY 1, 2 HAVING COUNT(*) >= 10 ORDER BY loyalty_conversion DESC;

-- 2) Cohort retention by signup month
CREATE OR REPLACE VIEW grocery.v_cohort_retention AS
SELECT DATE_TRUNC(c.signup_date, MONTH) AS cohort,
       DATE_DIFF(DATE_TRUNC(s.sale_date, MONTH), DATE_TRUNC(c.signup_date, MONTH), MONTH) AS months_since_signup,
       COUNT(DISTINCT s.customer_id) AS active_customers
FROM grocery.dim_customer c JOIN grocery.fact_sales s USING (customer_id)
GROUP BY 1, 2;

-- 3) Area-level opportunity: cities with low penetration but high basket value
CREATE OR REPLACE VIEW grocery.v_city_performance AS
SELECT c.city, COUNT(DISTINCT c.customer_id) AS customers, SUM(s.revenue) AS revenue,
       SAFE_DIVIDE(SUM(s.revenue), COUNT(DISTINCT s.order_id)) AS avg_order_value,
       SAFE_DIVIDE(SUM(s.revenue), COUNT(DISTINCT c.customer_id)) AS revenue_per_customer
FROM grocery.dim_customer c LEFT JOIN grocery.fact_sales s USING (customer_id) GROUP BY 1;

-- 4) Basket-builder: bundle ideas from strong affinity pairs (use for combo offers & shelf placement)
CREATE OR REPLACE VIEW grocery.v_bundle_ideas AS
SELECT a.p1, p1.product_name AS product_1, a.p2, p2.product_name AS product_2, a.lift, a.together
FROM grocery.product_affinity a
JOIN grocery.dim_product p1 ON p1.product_id = a.p1
JOIN grocery.dim_product p2 ON p2.product_id = a.p2
WHERE a.p1 < a.p2 ORDER BY a.lift DESC LIMIT 50;

-- 5) Slow movers -> clearance candidates (high stock, low demand, short shelf life)
CREATE OR REPLACE VIEW grocery.v_clearance_candidates AS
SELECT p.product_id, p.product_name, p.shelf_life_days, s.closing_stock,
       d.avg_daily_units, SAFE_DIVIDE(s.closing_stock, d.avg_daily_units) AS days_of_cover
FROM grocery.dim_product p
JOIN (SELECT product_id, closing_stock FROM grocery.raw_availability
      QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) = 1) s USING (product_id)
JOIN (SELECT product_id, AVG(units) AS avg_daily_units FROM grocery.daily_product_demand
      WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) GROUP BY 1) d USING (product_id)
WHERE SAFE_DIVIDE(s.closing_stock, d.avg_daily_units) > p.shelf_life_days * 0.6;

-- ---------- Campaign queue consumed by the email job ----------
CREATE OR REPLACE VIEW grocery.v_email_campaign_queue AS
SELECT c.customer_id, c.name, c.email, r.segment,
       ARRAY_AGG(STRUCT(o.product_name, o.selling_price, o.offer_price, o.discount_pct, o.reason, o.theme)
                 ORDER BY o.rk LIMIT 5) AS offers
FROM grocery.dim_customer c
JOIN grocery.customer_rfm r USING (customer_id)
JOIN grocery.customer_offers o USING (customer_id)
WHERE c.marketing_opt_in AND c.email IS NOT NULL
GROUP BY 1, 2, 3, 4;
