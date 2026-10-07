view: fact_sales {
  sql_table_name: `grocery.fact_sales` ;;

  dimension: transaction_id { primary_key: yes  type: string  sql: ${TABLE}.transaction_id ;; }
  dimension: order_id { type: string  sql: ${TABLE}.order_id ;; }
  dimension: customer_id { type: string  hidden: yes  sql: ${TABLE}.customer_id ;; }
  dimension: product_id { type: string  hidden: yes  sql: ${TABLE}.product_id ;; }
  dimension: sale_hour { type: number  sql: ${TABLE}.sale_hour ;; }
  dimension: festival { type: string  sql: IFNULL(${TABLE}.festival, 'Regular day') ;; }
  dimension_group: sale { type: time  timeframes: [date, week, month, quarter, year, day_of_week]  datatype: date  sql: ${TABLE}.sale_date ;; }

  measure: revenue { type: sum  sql: ${TABLE}.revenue ;;  value_format: "[$₹]#,##0" }
  measure: cogs { type: sum  sql: ${TABLE}.cogs ;;  value_format: "[$₹]#,##0" }
  measure: gross_profit { type: sum  sql: ${TABLE}.gross_profit ;;  value_format: "[$₹]#,##0" }
  measure: discount_given { type: sum  sql: ${TABLE}.discount_given ;;  value_format: "[$₹]#,##0" }
  measure: units { type: sum  sql: ${TABLE}.quantity ;; }
  measure: orders { type: count_distinct  sql: ${order_id} ;; }
  measure: avg_order_value { type: number  sql: SAFE_DIVIDE(${revenue}, ${orders}) ;;  value_format: "[$₹]#,##0" }
  measure: gross_margin_pct { type: number  sql: SAFE_DIVIDE(${gross_profit}, ${revenue}) ;;  value_format_name: percent_1 }
}

view: dim_product {
  sql_table_name: `grocery.dim_product` ;;
  dimension: product_id { primary_key: yes  type: string  sql: ${TABLE}.product_id ;; }
  dimension: product_name { type: string  sql: ${TABLE}.product_name ;; }
  dimension: category { type: string  sql: ${TABLE}.category ;; }
  dimension: supplier { type: string  sql: ${TABLE}.supplier ;; }
}

view: dim_customer {
  sql_table_name: `grocery.dim_customer` ;;
  dimension: customer_id { primary_key: yes  type: string  sql: ${TABLE}.customer_id ;; }
  dimension: name { type: string  sql: ${TABLE}.name ;; }
  dimension: city { type: string  sql: ${TABLE}.city ;; }
  dimension_group: signup { type: time  timeframes: [date, month]  datatype: date  sql: ${TABLE}.signup_date ;; }
}

view: customer_rfm {
  sql_table_name: `grocery.customer_rfm` ;;
  dimension: customer_id { primary_key: yes  hidden: yes  type: string  sql: ${TABLE}.customer_id ;; }
  dimension: segment { type: string  sql: ${TABLE}.segment ;; }
  dimension: recency_days { type: number  sql: ${TABLE}.recency_days ;; }
  measure: customers { type: count }
  measure: avg_monetary { type: average  sql: ${TABLE}.monetary ;; }
}

view: monthly_pnl {
  sql_table_name: `grocery.v_monthly_pnl` ;;
  dimension_group: month { type: time  timeframes: [month, quarter, year]  datatype: date  sql: ${TABLE}.month ;; }
  dimension: status { type: string  sql: ${TABLE}.status ;; }
  measure: revenue { type: sum  sql: ${TABLE}.revenue ;; }
  measure: gross_profit { type: sum  sql: ${TABLE}.gross_profit ;; }
  measure: opex { type: sum  sql: ${TABLE}.opex ;; }
  measure: net_profit { type: sum  sql: ${TABLE}.net_profit ;; }
}

view: hourly_traffic {
  sql_table_name: `grocery.v_hourly_traffic` ;;
  dimension: sale_hour { primary_key: yes  type: number  sql: ${TABLE}.sale_hour ;; }
  measure: avg_orders_per_day { type: sum  sql: ${TABLE}.avg_orders_per_day ;; }
  measure: avg_revenue_per_day { type: sum  sql: ${TABLE}.avg_revenue_per_day ;; }
}

view: inventory_plan {
  sql_table_name: `grocery.inventory_plan` ;;
  dimension: product_id { primary_key: yes  type: string  sql: ${TABLE}.product_id ;; }
  dimension: product_name { type: string  sql: ${TABLE}.product_name ;; }
  dimension: category { type: string  sql: ${TABLE}.category ;; }
  dimension: festival { type: string  sql: ${TABLE}.festival ;; }
  dimension: supplier { type: string  sql: ${TABLE}.supplier ;; }
  measure: current_stock { type: sum  sql: ${TABLE}.current_stock ;; }
  measure: target_stock { type: sum  sql: ${TABLE}.target_stock ;; }
  measure: suggested_order_qty { type: sum  sql: ${TABLE}.suggested_order_qty ;; }
}

view: product_profitability {
  sql_table_name: `grocery.v_product_profitability` ;;
  dimension: product_id { primary_key: yes  type: string  sql: ${TABLE}.product_id ;; }
  dimension: product_name { type: string  sql: ${TABLE}.product_name ;; }
  dimension: category { type: string  sql: ${TABLE}.category ;; }
  dimension: is_loss_making { type: yesno  sql: ${TABLE}.is_loss_making ;; }
  measure: gross_profit { type: sum  sql: ${TABLE}.gross_profit ;; }
  measure: revenue { type: sum  sql: ${TABLE}.revenue ;; }
}
