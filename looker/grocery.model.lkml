## LookML for the grocery BigQuery dataset.
## Create a BigQuery connection named "grocery_bq" in Looker, then add this as a project.

connection: "grocery_bq"
include: "/views/*.view.lkml"

explore: sales {
  from: fact_sales
  label: "Sales & Profit"
  join: product { from: dim_product  sql_on: ${sales.product_id} = ${product.product_id} ;; relationship: many_to_one }
  join: customer { from: dim_customer sql_on: ${sales.customer_id} = ${customer.customer_id} ;; relationship: many_to_one }
  join: rfm { from: customer_rfm sql_on: ${sales.customer_id} = ${rfm.customer_id} ;; relationship: many_to_one }
}

explore: monthly_pnl {}
explore: hourly_traffic {}
explore: inventory_plan {}
explore: product_profitability {}
explore: customer_rfm {}
