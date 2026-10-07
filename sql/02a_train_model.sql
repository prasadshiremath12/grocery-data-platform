-- 02a_train_model.sql : (re)train the demand model. Airflow runs this weekly (Sunday) or when the model is missing.
-- ARIMA_PLUS learns trend, weekly + yearly seasonality and holiday effects (holiday_region 'IN' - change for your country).
-- With < 2 years of history, yearly seasonality cannot be learned; the explicit festival uplift in 02b covers that.

-- One time series per product (time_series_id_col)
CREATE OR REPLACE MODEL grocery.demand_forecast_model
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'sale_date',
  time_series_data_col = 'units',
  time_series_id_col = 'product_id',
  holiday_region = 'IN',
  auto_arima = TRUE,
  data_frequency = 'DAILY',
  decompose_time_series = TRUE
) AS
SELECT sale_date, product_id, units FROM grocery.daily_product_demand;

