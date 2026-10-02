"""Generate a small, messy SAP-style sales order extract (CSV) for the demo."""
import csv
import random
from datetime import date, timedelta

REGIONS = ["DE", "FR", "IN", "US", "NL"]
MATERIALS = {"M-100": 12.5, "M-200": 48.0, "M-300": 7.9, "M-400": 150.0}


def generate(path: str, rows: int = 500, seed: int = 42) -> None:
    rnd = random.Random(seed)
    start = date(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "order_date", "region", "material", "quantity", "net_price"])
        for i in range(rows):
            mat = rnd.choice(list(MATERIALS))
            d = start + timedelta(days=rnd.randint(0, 270))
            qty = rnd.randint(1, 20)
            price = round(MATERIALS[mat] * qty, 2)
            row = [f"SO{i:05d}", d.isoformat(), rnd.choice(REGIONS), mat, qty, price]
            # inject dirt: bad quantity, missing region, duplicate rows
            if i % 53 == 0:
                row[4] = -1
            if i % 71 == 0:
                row[2] = ""
            w.writerow(row)
            if i % 97 == 0:
                w.writerow(row)


if __name__ == "__main__":
    import sys
    generate(sys.argv[1] if len(sys.argv) > 1 else "data/sales_raw.csv")
