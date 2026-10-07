"""Schemas, parsing and validation for each CSV feed.

Each feed has: BigQuery schema, CSV column order, a row parser that raises ValueError on bad data.
"""
from datetime import datetime, date

TRUE = {"true", "1", "yes", "y"}

def _bool(v):
    return str(v).strip().lower() in TRUE

def _req(v, name):
    v = (v or "").strip()
    if not v:
        raise ValueError(f"missing {name}")
    return v


def parse_transaction(r):
    qty = int(r["quantity"])
    price = float(r["unit_price"])
    disc = float(r.get("discount_pct") or 0)
    if qty <= 0 or price < 0 or not (0 <= disc <= 100):
        raise ValueError("invalid qty/price/discount")
    ts = datetime.fromisoformat(_req(r["transaction_ts"], "transaction_ts"))
    return {
        "transaction_id": _req(r["transaction_id"], "transaction_id"),
        "order_id": _req(r["order_id"], "order_id"),
        "customer_id": _req(r["customer_id"], "customer_id"),
        "product_id": _req(r["product_id"], "product_id"),
        "quantity": qty,
        "unit_price": price,
        "discount_pct": disc,
        "payment_mode": (r.get("payment_mode") or "UNKNOWN").strip().upper(),
        "transaction_ts": ts.isoformat(sep=" "),
    }


def parse_customer(r):
    email = (r.get("email") or "").strip().lower()
    if email and "@" not in email:
        raise ValueError("bad email")
    return {
        "customer_id": _req(r["customer_id"], "customer_id"),
        "name": (r.get("name") or "").strip(),
        "email": email or None,
        "phone": (r.get("phone") or "").strip() or None,
        "city": (r.get("city") or "").strip() or None,
        "signup_date": date.fromisoformat(_req(r["signup_date"], "signup_date")).isoformat(),
        "marketing_opt_in": _bool(r.get("marketing_opt_in")),
    }


def parse_product(r):
    cost, price = float(r["cost_price"]), float(r["selling_price"])
    if cost < 0 or price < 0:
        raise ValueError("negative price")
    return {
        "product_id": _req(r["product_id"], "product_id"),
        "product_name": _req(r["product_name"], "product_name"),
        "category": _req(r["category"], "category"),
        "cost_price": cost,
        "selling_price": price,
        "supplier": (r.get("supplier") or "").strip() or None,
        "shelf_life_days": int(r["shelf_life_days"]) if r.get("shelf_life_days") else None,
    }


def parse_availability(r):
    opening, closing = int(r["opening_stock"]), int(r["closing_stock"])
    if opening < 0 or closing < 0:
        raise ValueError("negative stock")
    return {
        "snapshot_date": date.fromisoformat(_req(r["snapshot_date"], "snapshot_date")).isoformat(),
        "product_id": _req(r["product_id"], "product_id"),
        "opening_stock": opening,
        "closing_stock": closing,
        "stockout_flag": _bool(r.get("stockout_flag")),
        "reorder_level": int(r.get("reorder_level") or 0),
    }


def parse_expense(r):
    amt = float(r["amount"])
    if amt < 0:
        raise ValueError("negative amount")
    return {
        "expense_id": _req(r["expense_id"], "expense_id"),
        "expense_date": date.fromisoformat(_req(r["expense_date"], "expense_date")).isoformat(),
        "category": _req(r["category"], "category"),
        "amount": amt,
    }


def parse_festival(r):
    s = date.fromisoformat(_req(r["start_date"], "start_date"))
    e = date.fromisoformat(_req(r["end_date"], "end_date"))
    if e < s:
        raise ValueError("end_date before start_date")
    return {"festival": _req(r["festival"], "festival"), "start_date": s.isoformat(), "end_date": e.isoformat()}


FEEDS = {
    "festivals": {
        "file": "festivals.csv",
        "parser": parse_festival,
        "table": "raw_festivals",
        "bq_schema": "festival:STRING,start_date:DATE,end_date:DATE",
        "key": None,
    },
    "transactions": {
        "file": "transactions.csv",
        "parser": parse_transaction,
        "table": "raw_transactions",
        "bq_schema": "transaction_id:STRING,order_id:STRING,customer_id:STRING,product_id:STRING,quantity:INTEGER,"
                     "unit_price:NUMERIC,discount_pct:NUMERIC,payment_mode:STRING,transaction_ts:DATETIME",
        "key": "transaction_id",
    },
    "customers": {
        "file": "customers.csv",
        "parser": parse_customer,
        "table": "raw_customers",
        "bq_schema": "customer_id:STRING,name:STRING,email:STRING,phone:STRING,city:STRING,signup_date:DATE,marketing_opt_in:BOOLEAN",
        "key": "customer_id",
    },
    "products": {
        "file": "products.csv",
        "parser": parse_product,
        "table": "raw_products",
        "bq_schema": "product_id:STRING,product_name:STRING,category:STRING,cost_price:NUMERIC,selling_price:NUMERIC,"
                     "supplier:STRING,shelf_life_days:INTEGER",
        "key": "product_id",
    },
    "availability": {
        "file": "availability.csv",
        "parser": parse_availability,
        "table": "raw_availability",
        "bq_schema": "snapshot_date:DATE,product_id:STRING,opening_stock:INTEGER,closing_stock:INTEGER,"
                     "stockout_flag:BOOLEAN,reorder_level:INTEGER",
        "key": None,
    },
    "expenses": {
        "file": "expenses.csv",
        "parser": parse_expense,
        "table": "raw_expenses",
        "bq_schema": "expense_id:STRING,expense_date:DATE,category:STRING,amount:NUMERIC",
        "key": "expense_id",
    },
}
