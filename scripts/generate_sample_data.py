"""
Generate synthetic retail sample data for development and testing.
Produces customers.csv, orders.csv, and products.csv in data/sample/.
"""

import csv
import os
import random
from datetime import datetime, timedelta

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "sample")
os.makedirs(OUTPUT_DIR, exist_ok=True)

COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "AE", "SA", "SG", "JP"]
CATEGORIES = ["Electronics", "Apparel", "Footwear", "Accessories", "Home", "Sports"]
FIRST_NAMES = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia",
               "Benjamin", "Isabella", "Lucas", "Mia", "Henry", "Charlotte", "Alexander"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson"]


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_customers(n: int = 500) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        reg_date = random_date(datetime(2020, 1, 1), datetime(2024, 1, 1))
        customers.append({
            "customer_id": f"CUST{i:06d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{random.randint(1,99)}@example.com",
            "country": random.choice(COUNTRIES),
            "city": f"City_{random.randint(1, 50)}",
            "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            "registration_date": reg_date.strftime("%Y-%m-%d"),
            "is_active": random.choice([True, True, True, False]),
            "created_at": reg_date.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": random_date(reg_date, datetime(2024, 6, 1)).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return customers


def generate_products(n: int = 100) -> list[dict]:
    products = []
    for i in range(1, n + 1):
        cat = random.choice(CATEGORIES)
        products.append({
            "product_id": f"PROD{i:05d}",
            "product_name": f"{cat} Item {i}",
            "category": cat,
            "brand": f"Brand_{random.randint(1, 20)}",
            "unit_price": round(random.uniform(9.99, 499.99), 2),
            "cost_price": round(random.uniform(3.0, 200.0), 2),
            "stock_qty": random.randint(0, 1000),
            "is_active": True,
            "created_at": datetime(2020, 1, 1).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return products


def generate_orders(customers: list[dict], products: list[dict], n: int = 2000) -> list[dict]:
    orders = []
    for i in range(1, n + 1):
        customer = random.choice(customers)
        reg_dt = datetime.strptime(customer["registration_date"], "%Y-%m-%d")
        order_dt = random_date(reg_dt, datetime(2024, 6, 1))
        product = random.choice(products)
        qty = random.randint(1, 5)
        unit_price = float(product["unit_price"])
        discount = round(random.uniform(0, 0.3), 2)
        total = round(unit_price * qty * (1 - discount), 2)
        statuses = ["completed", "completed", "completed", "returned", "cancelled"]
        orders.append({
            "order_id": f"ORD{i:08d}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": qty,
            "unit_price": unit_price,
            "discount_pct": discount,
            "total_amount": total,
            "currency": "USD",
            "status": random.choice(statuses),
            "channel": random.choice(["web", "mobile", "store", "partner"]),
            "order_date": order_dt.strftime("%Y-%m-%d"),
            "shipped_date": (order_dt + timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d"),
            "country": customer["country"],
            "created_at": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": (order_dt + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return orders


def write_csv(records: list[dict], filename: str) -> None:
    path = os.path.join(OUTPUT_DIR, filename)
    if not records:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Written {len(records):,} records → {path}")


if __name__ == "__main__":
    random.seed(42)
    print("Generating sample data...")
    customers = generate_customers(500)
    products = generate_products(100)
    orders = generate_orders(customers, products, 2000)
    write_csv(customers, "customers.csv")
    write_csv(products, "products.csv")
    write_csv(orders, "orders.csv")
    print("Done.")
