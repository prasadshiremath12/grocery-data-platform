"""Generate realistic sample CSVs for the grocery platform.

Usage: python scripts/generate_sample_data.py --out data/ --customers 500 --days 730
Schemas (header rows) match pipeline/schemas.py.
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

random.seed(42)

CATEGORIES = {
    "Staples": [("Basmati Rice 5kg", 420, 480), ("Wheat Flour 5kg", 210, 245), ("Toor Dal 1kg", 130, 155), ("Sugar 1kg", 42, 50)],
    "Dairy": [("Milk 1L", 52, 58), ("Paneer 200g", 78, 90), ("Curd 500g", 32, 38), ("Ghee 500ml", 290, 335)],
    "Snacks": [("Namkeen 400g", 85, 105), ("Biscuits Pack", 18, 22), ("Chips Large", 40, 50)],
    "Beverages": [("Tea 500g", 210, 250), ("Coffee 200g", 270, 320), ("Fruit Juice 1L", 85, 105)],
    "Festival": [("Dry Fruits Box 500g", 480, 620), ("Sweets Gift Box", 350, 480), ("Puja Oil 1L", 140, 170), ("Diya Pack of 12", 60, 90)],
    "Fresh": [("Tomato 1kg", 25, 40), ("Onion 1kg", 28, 42), ("Banana Dozen", 40, 60)],
}

# (name, start month-day, end month-day, multiplier for Festival/Snacks/Dry fruits)
FESTIVALS = [("Diwali", date(2024, 10, 28), date(2024, 11, 3)), ("Diwali", date(2025, 10, 17), date(2025, 10, 23)),
             ("Diwali", date(2026, 11, 5), date(2026, 11, 11)),
             ("Ganesh Chaturthi", date(2024, 9, 5), date(2024, 9, 12)), ("Ganesh Chaturthi", date(2025, 8, 25), date(2025, 9, 1)),
             ("Ugadi", date(2025, 3, 28), date(2025, 3, 30)), ("Ugadi", date(2026, 3, 18), date(2026, 3, 20))]

HOUR_WEIGHTS = {7: 3, 8: 5, 9: 6, 10: 5, 11: 4, 12: 4, 13: 3, 14: 2, 15: 3, 16: 5, 17: 9, 18: 12, 19: 11, 20: 6, 21: 2}


def in_festival(d):
    return any(s - timedelta(days=5) <= d <= e for _, s, e in FESTIVALS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data")
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--days", type=int, default=730)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    products = []
    pid = 1
    for cat, items in CATEGORIES.items():
        for name, cost, price in items:
            products.append(dict(product_id=f"P{pid:04d}", product_name=name, category=cat,
                                 cost_price=cost, selling_price=price, supplier=f"Supplier{random.randint(1, 6)}",
                                 shelf_life_days=random.choice([3, 7, 30, 180, 365]) if cat != "Fresh" else 4))
            pid += 1

    cities = ["Belagavi", "Hubballi", "Dharwad", "Kolhapur"]
    customers = []
    for i in range(1, a.customers + 1):
        customers.append(dict(customer_id=f"C{i:05d}", name=f"Customer {i}", email=f"customer{i}@example.com",
                              phone=f"9{random.randint(100000000, 999999999)}", city=random.choice(cities),
                              signup_date=(date.today() - timedelta(days=random.randint(10, 900))).isoformat(),
                              marketing_opt_in=random.choice(["true", "true", "true", "false"])))

    end = date.today()
    start = end - timedelta(days=a.days)
    txns, avail = [], []
    tid = 1
    hours, hw = list(HOUR_WEIGHTS), list(HOUR_WEIGHTS.values())
    d = start
    while d <= end:
        fest = in_festival(d)
        weekend = d.weekday() >= 5
        n_orders = int(random.gauss(60, 8) * (1.25 if weekend else 1) * (2.2 if fest else 1))
        stock = {p["product_id"]: random.randint(80, 200) for p in products}
        sold = {p["product_id"]: 0 for p in products}
        for _ in range(n_orders):
            c = random.choice(customers)
            h = random.choices(hours, hw)[0]
            ts = datetime(d.year, d.month, d.day, h, random.randint(0, 59), random.randint(0, 59))
            for _ in range(random.randint(1, 6)):
                weights = [(3 if (fest and p["category"] in ("Festival", "Snacks")) else 1) for p in products]
                p = random.choices(products, weights)[0]
                qty = random.randint(1, 4)
                disc = random.choice([0, 0, 0, 5, 10]) / 100
                txns.append(dict(transaction_id=f"T{tid:08d}", order_id=f"O{d:%y%m%d}{h:02d}{random.randint(0, 9999):04d}",
                                 customer_id=c["customer_id"], product_id=p["product_id"], quantity=qty,
                                 unit_price=p["selling_price"], discount_pct=round(disc * 100, 1),
                                 payment_mode=random.choice(["UPI", "Cash", "Card"]),
                                 transaction_ts=ts.isoformat(sep=" ")))
                sold[p["product_id"]] += qty
                tid += 1
        for p in products:
            opening = stock[p["product_id"]]
            avail.append(dict(snapshot_date=d.isoformat(), product_id=p["product_id"], opening_stock=opening,
                              closing_stock=max(opening - sold[p["product_id"]], 0),
                              stockout_flag="true" if sold[p["product_id"]] >= opening else "false",
                              reorder_level=40))
        d += timedelta(days=1)

    def dump(name, rows):
        with open(os.path.join(a.out, name), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        print(f"{name}: {len(rows)} rows")

    dump("products.csv", products)
    dump("customers.csv", customers)
    dump("transactions.csv", txns)
    dump("availability.csv", avail)

    # Operating expenses (monthly): rent, salaries, utilities, wastage
    exp = []
    m = date(start.year, start.month, 1)
    while m <= end:
        for cat, amt in [("Rent", 60000), ("Salaries", 150000), ("Utilities", 25000), ("Wastage", 18000), ("Marketing", 12000)]:
            exp.append(dict(expense_id=f"E{len(exp) + 1:06d}", expense_date=m.isoformat(), category=cat,
                            amount=round(amt * random.uniform(0.92, 1.1), 2)))
        m = date(m.year + (m.month == 12), (m.month % 12) + 1, 1)
    dump("expenses.csv", exp)
    dump("festivals.csv", [dict(festival=n, start_date=(s0 - timedelta(days=5)).isoformat(), end_date=e0.isoformat()) for n, s0, e0 in FESTIVALS])


if __name__ == "__main__":
    main()
