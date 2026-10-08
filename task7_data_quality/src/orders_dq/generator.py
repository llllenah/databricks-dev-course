import random
from datetime import datetime, timedelta

from .quality.rules import ACCEPTED_CHANNELS

CUSTOMERS = 50

BAD_RECORD_KINDS = (
    "null_order_id",
    "null_customer",
    "negative_amount",
    "amount_too_large",
    "amount_not_numeric",
    "bad_timestamp",
    "future_timestamp",
    "unknown_channel",
    "bad_customer_format",
    "duplicate_order_id",
)


def _fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def generate_orders(n_valid: int = 1000, bad_per_kind: int = 3, end: datetime | None = None,
                    seed: int = 42) -> list[dict]:
    rnd = random.Random(seed)
    end = (end or datetime.now()).replace(microsecond=0)
    rows = []
    for i in range(n_valid):
        ts = end - timedelta(minutes=n_valid - i)
        rows.append({
            "order_id": str(i + 1),
            "customer": f"cust_{rnd.randrange(CUSTOMERS)}",
            "channel": rnd.choice(ACCEPTED_CHANNELS),
            "amount": f"{rnd.uniform(5, 200):.2f}",
            "ts": _fmt(ts),
        })

    next_id = n_valid + 1
    for kind in BAD_RECORD_KINDS:
        for _ in range(bad_per_kind):
            base = dict(rnd.choice(rows[:n_valid]))
            base["order_id"] = str(next_id)
            next_id += 1
            if kind == "null_order_id":
                base["order_id"] = None
            elif kind == "null_customer":
                base["customer"] = None
            elif kind == "negative_amount":
                base["amount"] = f"-{rnd.uniform(1, 50):.2f}"
            elif kind == "amount_too_large":
                base["amount"] = "250000.00"
            elif kind == "amount_not_numeric":
                base["amount"] = "12,5O"
            elif kind == "bad_timestamp":
                base["ts"] = "31/02/2026 25:61"
            elif kind == "future_timestamp":
                base["ts"] = _fmt(end + timedelta(days=30))
            elif kind == "unknown_channel":
                base["channel"] = "fax"
            elif kind == "bad_customer_format":
                base["customer"] = "customer #" + str(rnd.randrange(CUSTOMERS))
            elif kind == "duplicate_order_id":
                original = rows[rnd.randrange(n_valid)]
                base = dict(original)
            rows.append(base)
    rnd.shuffle(rows)
    return rows


def split_batches(rows: list[dict], first_share: float = 0.6) -> list[list[dict]]:
    cut = int(len(rows) * first_share)
    return [rows[:cut], rows[cut:]]
