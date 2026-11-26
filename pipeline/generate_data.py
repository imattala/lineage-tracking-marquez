"""Generate synthetic customers/orders CSVs for the ingestion step.

Deliberately not OpenLineage-instrumented - this is just test-data prep,
not part of the pipeline whose lineage we're tracking.
"""

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

DATA_DIR = Path(__file__).parent / "data"
N_CUSTOMERS = 200
N_ORDERS = 1000

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_customers() -> list[int]:
    path = DATA_DIR / "customers.csv"
    customer_ids = list(range(1, N_CUSTOMERS + 1))
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "email", "created_at"])
        for customer_id in customer_ids:
            created_at = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
            writer.writerow([customer_id, fake.name(), fake.email(), created_at.isoformat()])
    print(f"wrote {len(customer_ids)} rows to {path}")
    return customer_ids


def generate_orders(customer_ids: list[int]) -> None:
    path = DATA_DIR / "orders.csv"
    statuses = ["pending", "shipped", "delivered", "cancelled"]
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "amount", "status"])
        for order_id in range(1, N_ORDERS + 1):
            order_date = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 180))
            writer.writerow(
                [
                    order_id,
                    random.choice(customer_ids),
                    order_date.isoformat(),
                    round(random.uniform(5, 500), 2),
                    random.choice(statuses),
                ]
            )
    print(f"wrote {N_ORDERS} rows to {path}")


if __name__ == "__main__":
    DATA_DIR.mkdir(exist_ok=True)
    customer_ids = generate_customers()
    generate_orders(customer_ids)
