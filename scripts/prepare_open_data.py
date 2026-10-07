"""Convert the open-source dunnhumby "Complete Journey" grocery dataset into this project's CSV feeds.

Real data (from the source):  transactions (household, basket, product, qty, sales value, discounts, timestamp),
                              product hierarchy (department / category / brand / size), household demographics.
Not in the source, so SIMULATED here (clearly flagged below, deterministic via --seed):
  cost_price, shelf_life_days, stock availability, operating expenses, e-mail addresses, opt-in flags, festival calendar.

Usage:
  python scripts/prepare_open_data.py --fetch --out data/open            # clones the dataset from GitHub, writes CSVs
  python scripts/prepare_open_data.py --source /path/to/completejourney_py/data --out data/open

Dataset: https://github.com/cunningjames/completejourney_py (data originates from dunnhumby, released for
research / testing use). Use it to build and test the pipeline only - not for your production reporting.
"""
import argparse
import csv
import hashlib
import math
import os
import random
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parquet_lite import read_columns  # noqa: E402

REPO = "https://github.com/cunningjames/completejourney_py"
DATA_SUBDIR = os.path.join("completejourney_py", "data")

MARGIN = {"GROCERY": 0.25, "PRODUCE": 0.33, "MEAT": 0.20, "MEAT-PCKGD": 0.22, "DELI": 0.35, "PASTRY": 0.40,
          "DRUG GM": 0.30, "NUTRITION": 0.32, "SEAFOOD-PCKGD": 0.24, "SEAFOOD": 0.24, "COSMETICS": 0.38}
PERISHABLE_WORDS = ("MILK", "YOGURT", "EGG", "BREAD", "CHEESE", "SALAD", "FRUIT", "VEG", "BAKERY", "DELI")


def h(*parts):
    return int(hashlib.md5("|".join(map(str, parts)).encode()).hexdigest()[:8], 16)


def nth_weekday(year, month, weekday, n):
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def last_weekday(year, month, weekday):
    d = date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def festival_windows(year):
    """US retail peaks that really show up in this data (the original dataset is US supermarket)."""
    tg = nth_weekday(year, 11, 3, 4)
    mem = last_weekday(year, 5, 0)
    return [("Thanksgiving", tg - timedelta(days=6), tg),
            ("Christmas", date(year, 12, 18), date(year, 12, 25)),
            ("July 4th", date(year, 6, 30), date(year, 7, 4)),
            ("Memorial Day", mem - timedelta(days=3), mem)]


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"  {os.path.basename(path)}: {len(rows):,} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="folder with transactions.parquet, products.parquet, demographics.parquet")
    ap.add_argument("--fetch", action="store_true", help="git clone the dataset into <out>/_source if --source not given")
    ap.add_argument("--out", default="data/open")
    ap.add_argument("--top_products", type=int, default=500, help="keep the N best-selling products (keeps BQML forecasting cheap)")
    ap.add_argument("--no_shift", action="store_true", help="keep original 2017-2018 dates")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    random.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)

    src = a.source
    if not src:
        if not a.fetch:
            sys.exit("give --source <dir> or --fetch")
        clone = os.path.join(a.out, "_source")
        if not os.path.isdir(clone):
            subprocess.run(["git", "clone", "--depth", "1", REPO, clone], check=True)
        src = os.path.join(clone, DATA_SUBDIR)

    print("Reading open dataset ...")
    t = read_columns(os.path.join(src, "transactions.parquet"),
                     ["household_id", "store_id", "basket_id", "product_id", "quantity", "sales_value",
                      "retail_disc", "transaction_timestamp"])
    prod = read_columns(os.path.join(src, "products.parquet"))
    demo = read_columns(os.path.join(src, "demographics.parquet"))
    n = len(t["product_id"])
    print(f"  {n:,} raw line items")

    # --- keep valid sales lines of the top-N products ---
    # drop non-merchandise lines (fuel, coupons, misc. transactions) - they have no meaningful shelf price/stock
    pidx0 = {p: j for j, p in enumerate(prod["product_id"])}
    junk_dept = {"MISCELLANEOUS", "FUEL", "KIOSK-GAS", "COUPON", "MISC. TRANS.", "MISC SALES TRAN", "UNKNOWN"}

    def is_merch(pid):
        j = pidx0.get(pid)
        if j is None:
            return False
        dept = (prod["department"][j] or "").upper()
        cat = (prod["product_category"][j] or "").upper()
        return dept not in junk_dept and "COUPON" not in cat and "GASOLINE" not in (prod["product_type"][j] or "").upper()

    units = Counter()
    for i in range(n):
        if t["quantity"][i] > 0 and t["sales_value"][i] > 0 and is_merch(t["product_id"][i]):
            units[t["product_id"][i]] += t["quantity"][i]
    keep = {p for p, _ in units.most_common(a.top_products)} if a.top_products else set(units)

    tz0 = datetime(1970, 1, 1)
    rows_idx = [i for i in range(n) if t["product_id"][i] in keep and t["quantity"][i] > 0 and t["sales_value"][i] > 0]
    stamps = [tz0 + timedelta(microseconds=t["transaction_timestamp"][i]) for i in rows_idx]
    first, last = min(stamps).date(), max(stamps).date()

    # --- shift dates so the data ends "yesterday" (keeps weekdays) -> recency, forecasts and "next festival" work ---
    shift = 0
    if not a.no_shift:
        delta = (date.today() - timedelta(days=1) - last).days
        shift = delta - delta % 7
    sh = timedelta(days=shift)
    print(f"  date range {first} .. {last}; shifted by {shift} days -> {first + sh} .. {last + sh}")

    # --- transactions ---
    txns, price_acc, net_acc, store_by_hh = [], defaultdict(list), defaultdict(list), defaultdict(Counter)
    first_day = {}
    daily_units = defaultdict(lambda: defaultdict(int))
    for k, (i, ts) in enumerate(zip(rows_idx, stamps), 1):
        qty = t["quantity"][i]
        rd = max(t["retail_disc"][i], 0.0)  # source stores discounts as positive amounts
        gross = t["sales_value"][i] + rd
        unit = gross / qty
        pct = min(rd / gross * 100, 100) if gross else 0
        ts2 = ts + sh
        hh, pid = t["household_id"][i], t["product_id"][i]
        txns.append({"transaction_id": f"T{k:08d}", "order_id": f"B{t['basket_id'][i]}", "customer_id": f"H{hh:04d}",
                     "product_id": f"P{pid}", "quantity": qty, "unit_price": round(unit, 2),
                     "discount_pct": round(pct, 1), "payment_mode": random.choice(["CARD", "CARD", "CASH", "UPI"]),
                     "transaction_ts": ts2.isoformat(sep=" ")})
        price_acc[pid].append(unit)
        net_acc[pid].append(t["sales_value"][i] / qty)  # what the shop actually received per unit
        store_by_hh[hh][t["store_id"][i]] += 1
        d = ts2.date()
        if hh not in first_day or d < first_day[hh]:
            first_day[hh] = d
        daily_units[pid][d] += qty

    # --- products (cost / shelf life simulated; cost = typical NET selling price less a department margin) ---
    pidx = {p: j for j, p in enumerate(prod["product_id"])}
    products = []
    for pid in sorted(price_acc):
        j = pidx.get(pid)
        dept = (prod["department"][j] if j is not None else None) or "UNKNOWN"
        cat = (prod["product_category"][j] if j is not None else None) or dept
        ptype = (prod["product_type"][j] if j is not None else None) or cat
        brand = (prod["brand"][j] if j is not None else None) or ""
        size = (prod["package_size"][j] if j is not None else None) or ""
        name = " ".join(x for x in (brand if brand != "National" else "", ptype.title(), size.lower()) if x).strip()
        price = round(statistics.mean(price_acc[pid]), 2)
        margin = max(MARGIN.get(dept, 0.28) + ((h(pid) % 11) - 5) / 100, 0.08)
        perish = dept in ("PRODUCE", "MEAT", "DELI", "PASTRY", "SEAFOOD", "SEAFOOD-PCKGD") or any(w in cat.upper() for w in PERISHABLE_WORDS)
        life = {"PASTRY": 3, "PRODUCE": 5, "MEAT": 5, "SEAFOOD": 3, "DELI": 7}.get(dept, 7 if perish else (730 if dept in ("DRUG GM", "COSMETICS", "GM MERCH") else 180))
        mid = prod["manufacturer_id"][j] if j is not None else 0
        products.append({"product_id": f"P{pid}", "product_name": name or f"Product {pid}", "category": f"{dept}/{cat}" if cat != dept else dept,
                         "cost_price": round(statistics.mean(net_acc[pid]) * (1 - margin), 2), "selling_price": price,
                         "supplier": f"Supplier{mid}", "shelf_life_days": life})

    # --- customers (names/e-mails are synthetic placeholders; demographics stay in the source) ---
    demo_ids = set(demo["household_id"])
    customers = []
    for hh in sorted(first_day):
        city_store = store_by_hh[hh].most_common(1)[0][0]
        customers.append({"customer_id": f"H{hh:04d}", "name": f"Household {hh}", "email": f"household{hh}@example.invalid",
                          "phone": "", "city": f"Store {city_store}", "signup_date": first_day[hh].isoformat(),
                          "marketing_opt_in": "true" if h(hh, "optin") % 10 < 8 else "false"})

    # --- availability: simulated reorder policy driven by REAL daily demand (2-day lead time) ---
    d0, d1 = first + sh, last + sh
    ndays = (d1 - d0).days + 1
    avail = []
    for pid in sorted(price_acc):
        dem = daily_units[pid]
        avg = max(sum(dem.values()) / ndays, 0.2)
        reorder, up_to = math.ceil(3 * avg) + 3, math.ceil(9 * avg) + 6
        stock, arrivals = up_to, {}
        for k in range(ndays):
            day = d0 + timedelta(days=k)
            stock += arrivals.pop(day, 0)
            opening, demand = stock, dem.get(day, 0)
            stock = max(opening - demand, 0)
            avail.append({"snapshot_date": day.isoformat(), "product_id": f"P{pid}", "opening_stock": opening,
                          "closing_stock": stock, "stockout_flag": "true" if demand >= opening and demand > 0 else "false",
                          "reorder_level": reorder})
            pending = sum(arrivals.values())
            if stock + pending <= reorder:
                arrivals[day + timedelta(days=2)] = arrivals.get(day + timedelta(days=2), 0) + up_to - stock - pending

    # --- expenses (simulated as shares of monthly revenue + fixed rent) ---
    rev = defaultdict(float)
    for r in txns:
        rev[r["transaction_ts"][:7]] += r["quantity"] * r["unit_price"] * (1 - r["discount_pct"] / 100)
    avg_rev = statistics.mean(rev.values())
    expenses = []
    for ym, v in sorted(rev.items()):
        for cat, amt in [("Rent", 0.06 * avg_rev), ("Salaries", max(0.09 * v, 0.07 * avg_rev)), ("Utilities", 0.02 * avg_rev),
                         ("Wastage", 0.015 * v), ("Marketing", 0.01 * v)]:
            expenses.append({"expense_id": f"E{len(expenses) + 1:06d}", "expense_date": f"{ym}-01",
                             "category": cat, "amount": round(amt * random.uniform(0.93, 1.07), 2)})

    # --- festival calendar: windows inside the data (with 45d of history before) + one upcoming occurrence each ---
    fest = []
    for y in range(first.year, last.year + 1):
        for name, s, e in festival_windows(y):
            if s >= first + timedelta(days=45) and e <= last:
                fest.append((name, s + sh, e + sh))
    today = date.today()
    for name in {f[0] for f in fest}:
        latest = max((f for f in fest if f[0] == name), key=lambda f: f[1])
        s, e = latest[1], latest[2]
        while e < today:
            s, e = s + timedelta(days=364), e + timedelta(days=364)
        fest.append((name, s, e))
    festivals = [{"festival": n_, "start_date": s.isoformat(), "end_date": e.isoformat()} for n_, s, e in sorted(fest, key=lambda f: f[1])]

    print("Writing CSVs ...")
    write_csv(os.path.join(a.out, "transactions.csv"), txns)
    write_csv(os.path.join(a.out, "customers.csv"), customers)
    write_csv(os.path.join(a.out, "products.csv"), products)
    write_csv(os.path.join(a.out, "availability.csv"), avail)
    write_csv(os.path.join(a.out, "expenses.csv"), expenses)
    write_csv(os.path.join(a.out, "festivals.csv"), festivals)

    total_rev = sum(rev.values())
    print(f"\nSummary: revenue {total_rev:,.0f}, households {len(customers):,} ({len(demo_ids & set(first_day)):,} with source demographics), "
          f"products {len(products):,}, months {len(rev)}")


if __name__ == "__main__":
    main()
