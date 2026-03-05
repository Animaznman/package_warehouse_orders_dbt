"""Utility for generating synthetic warehouse inventory and order data in CSV format.

The generated table simulates a monolithic warehouse data table with fields
ranging from item identifiers to order/shipment dates.  It supports injection
of random errors (e.g. negative prices, malformed dates, missing values) and
can be customized in terms of categories, date range, number of rows, etc.

A simple command‑line interface is provided so the script can be run directly
from the shell.  When used as a library the :func:`generate_warehouse_data`
function returns a list of dictionaries (one per row) and/or writes a CSV.

Example usage::

    from data_generation.generate_warehouse_data import generate_warehouse_data

    rows = generate_warehouse_data(num_rows=1000,
                                   categories=["Toys","Books"],
                                   earliest_date="2025-01-01",
                                   latest_date="2025-12-31",
                                   erroneous=True,
                                   output_file="sample.csv")

"""

import argparse
import csv
import os
import random
import string
import uuid
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Union


# default categories if none provided
_DEFAULT_CATEGORIES = [
    "Computers&Accessories",
    "Electronics",
    "HomeGoods",
    "Clothing",
    "Sports",
    "Books",
]

# canonical field ordering; used for error injection when simulating
# transposition/column‑shift mistakes.  The order here must match the
# keys used when building each record in :func:`generate_warehouse_data`.
COLUMN_ORDER = [
    "ItemHash",
    "SkuNumber",
    "SoldFlag",
    "ItemName",
    "Price",
    "WarehouseId",
    "Category",
    "OrderDate",
    "ShipDate",
    "OutDate",
    "Delivery_date",
    "OrderId",
    "Quantity",
    "CustomerId",
]


def _random_hash(length: int = 12) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _random_brand_code() -> str:
    return "".join(random.choices(string.ascii_uppercase, k=3))


def _random_warehouse_id() -> str:
    return "WH" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))


def _random_item_name(category: str) -> str:
    # remove spaces and ampersands for name clarity
    clean_cat = category.replace(" ", "").replace("&", "and")
    return f"{clean_cat}-{_random_brand_code()}-{random.randint(1,999):03d}"


def _random_price() -> float:
    # realistic price range
    return round(random.uniform(1.0, 5000.0), 2)


def _random_sku() -> int:
    return random.randint(100000, 999999)


def _random_quantity() -> int:
    return random.randint(1, 10)


def _random_customer_id() -> str:
    return str(random.randint(1000000, 9999999))


def _random_date_between(start: date, end: date) -> date:
    delta = end - start
    if delta.days <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta.days))


def _bimodal_delivery_delta() -> timedelta:
    # choose one of two modes: around 2 days or around 10 days
    if random.random() < 0.5:
        days = max(1, int(random.gauss(2, 1)))
    else:
        days = max(1, int(random.gauss(10, 2)))
    # enforce maximum of 14
    days = min(days, 14)
    return timedelta(days=days)


def _possibly_inject_error(
    value,
    column: str,
    error_rate: float = .0001,
    record: Optional[dict] = None,
    rows: Optional[List[dict]] = None,
    row_idx: Optional[int] = None,
):
    """With probability `error_rate` mutate ``value`` to an erroneous form.

    Two additional error modes are available when extra context is given:

    * **horizontal shift** – if ``record`` is provided there is a chance the
      value from an adjacent column (per ``COLUMN_ORDER``) will be used
      instead.
    * **vertical shift** – if ``rows`` and ``row_idx`` are provided it is
      possible to take the value from another row in the same column.  The
      other row may either be swapped with the current one or cleared, leaving
      a ``None`` behind.  This simulates copy/paste mistakes between rows.
    """
    if random.random() >= error_rate:
        return value

    # vertical shift between rows (low chance)
    if rows is not None and row_idx is not None and rows:
        # only consider already-built rows; ``row_idx`` points to the index of
        # the current record once it is appended.  we choose a previous row to
        # avoid forward references.
        if random.random() < 0.1 and row_idx > 0:
            other_idx = random.randrange(0, row_idx)
            other_val = rows[other_idx].get(column)
            if random.random() < 0.5:
                # swap the two values
                rows[other_idx][column] = value
                return other_val
            else:
                # move the value and nullify the source
                rows[other_idx][column] = None
                return other_val

    # randomly shift value from a neighbouring column (small chance)
    if record is not None and random.random() < 0.2:
        try:
            idx = COLUMN_ORDER.index(column)
        except ValueError:
            idx = None
        if idx is not None:
            neighbours = []
            if idx > 0:
                neighbours.append(COLUMN_ORDER[idx - 1])
            if idx < len(COLUMN_ORDER) - 1:
                neighbours.append(COLUMN_ORDER[idx + 1])
            if neighbours:
                chosen = random.choice(neighbours)
                return record.get(chosen)

    # simple error strategies by column type
    if value is None:
        # already missing; introduce other error by returning invalid type
        return "ERROR"

    if column in ("Price",):
        # make negative or nonsensical string
        if isinstance(value, (int, float)):
            return -abs(value)
        return "-100"
    elif column in ("OrderDate", "ShipDate", "OutDate", "Delivery_date"):
        # sometimes swap month/day or return text
        if isinstance(value, (date, datetime)):
            fmt = "%m-%d-%Y" if random.random() < 0.5 else "%Y/%m/%d"
            return value.strftime(fmt)
        return "notadate"
    elif column in ("SkuNumber", "OrderId", "Quantity"):
        # return a string or negative
        if isinstance(value, int):
            return str(value)
        return -1
    elif column in ("ItemName", "Category", "WarehouseId", "CustomerId"):
        # misspell by shuffling letters or dropping characters
        if isinstance(value, str) and len(value) > 1:
            lst = list(value)
            i, j = random.sample(range(len(lst)), 2)
            lst[i], lst[j] = lst[j], lst[i]
            return "".join(lst)
        return ""
    else:
        # generic missing
        return None


def _maybe_none(value, flag: bool) -> Optional[object]:
    return None if flag else value


def generate_warehouse_data(
    num_rows: int = 2000,
    categories: Optional[Iterable[str]] = None,
    earliest_date: Optional[Union[str, date]] = None,
    latest_date: Optional[Union[str, date]] = None,
    erroneous: bool = False,
    error_rate: float = 0.001,
    output_file: Optional[str] = None,
    filename: str = "warehouse_data.csv",
) -> List[dict]:
    """Generate synthetic warehouse inventory/order records.

    Parameters
    ----------
    num_rows
        Number of rows to produce (default 2000).
    categories
        Iterable of category strings to sample from.  A sensible default list is
        used when ``None`` is provided.
    earliest_date
        Lower bound for order dates.  May be a :class:`datetime.date` or
        ISO-formatted string (``YYYY-MM-DD``).  Defaults to one year ago from
        today.
    latest_date
        Upper bound for order dates.  Defaults to today.
    erroneous
        If ``True`` randomly introduce errors into the output according to
        ``error_rate``.  If ``False`` the returned data will be clean.
    error_rate
        Probability that any given cell will be corrupted when ``erroneous``
        is ``True``.  Default is ``0.001``.
    output_file
        If provided, the generated rows will be written to this path as a CSV
        file.  The function still returns the list of dictionaries.
    filename
        Base name to use when ``output_file`` is not provided.  Defaults to
        ``warehouse_data.csv`` and is placed in ``~/data`` unless an alternate
        directory is specified via ``output_file``.

    Returns
    -------
    List[dict]
        A list containing the generated records.
    """
    if erroneous and error_rate <= 0:
        error_rate = 0.001
    if not erroneous:
        error_rate = 0.0

    cat_list = list(categories) if categories else _DEFAULT_CATEGORIES

    # parse date bounds
    today = date.today()
    if earliest_date is None:
        earliest = today - timedelta(days=365)
    elif isinstance(earliest_date, str):
        earliest = date.fromisoformat(earliest_date)
    else:
        earliest = earliest_date

    if latest_date is None:
        latest = today
    elif isinstance(latest_date, str):
        latest = date.fromisoformat(latest_date)
    else:
        latest = latest_date

    rows: List[dict] = []
    next_order_id = 1000000

    for _ in range(num_rows):
        sku = _random_sku()
        category = random.choice(cat_list)
        sold = random.choice((0, 1))

        order_date = None
        ship_date = None
        out_date = None
        delivery_date = None
        order_id = None

        if sold:
            order_date = _random_date_between(earliest, latest)
            ship_date = order_date + timedelta(days=random.randint(0, 7))
            delivery_date = order_date + _bimodal_delivery_delta()
            out_date = ship_date + timedelta(days=random.randint(0, 3))
            order_id = next_order_id
            next_order_id += 1

        quantity = _random_quantity()
        customer_id = _random_customer_id() if sold else None

        record = {
            "ItemHash": _random_hash(),
            "SkuNumber": sku,
            "SoldFlag": sold,
            "ItemName": _random_item_name(category),
            "Price": _random_price(),
            "WarehouseId": _random_warehouse_id(),
            "Category": category,
            "OrderDate": order_date,
            "ShipDate": ship_date,
            "OutDate": out_date,
            "Delivery_date": delivery_date,
            "OrderId": order_id,
            "Quantity": quantity,
            "CustomerId": customer_id,
        }

        # inject errors cell-wise; pass the whole record so _possibly_inject_error
        # can perform column-shift mistakes when appropriate.
        cols = list(record.keys())
        # pass current rows and the index we will occupy to support vertical errors
        current_index = len(rows)
        for col in cols:
            record[col] = _possibly_inject_error(
                record[col],
                col,
                error_rate,
                record,
                rows,
                current_index,
            )

        rows.append(record)

    # determine output path
    if output_file is None:
        default_dir = os.path.expanduser("./data")
        os.makedirs(default_dir, exist_ok=True)
        output_file = os.path.join(default_dir, filename)

    if output_file:
        # write to CSV, converting dates/datetimes to ISO format strings
        dirpath = os.path.dirname(output_file)
        if dirpath and not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)
        with open(output_file, "w", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for r in rows:
                out = {}
                for k, v in r.items():
                    if isinstance(v, (date, datetime)):
                        out[k] = v.isoformat()
                    else:
                        out[k] = v
                writer.writerow(out)
    return rows


def _cli():
    parser = argparse.ArgumentParser(description="Generate synthetic warehouse CSV data.")
    parser.add_argument("--rows", "-n", type=int, default=2000, help="number of rows")
    parser.add_argument("--categories", "-c", nargs="*", help="custom categories")
    parser.add_argument(
        "--earliest",
        type=str,
        help="earliest order date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--latest",
        type=str,
        help="latest order date (YYYY-MM-DD)",
    )
    parser.add_argument("--erroneous", action="store_true", help="include random errors")
    parser.add_argument(
        "--error-rate",
        type=float,
        default=0.001,
        help="per-cell error probability when --erroneous is set",
    )
    parser.add_argument("--output", "-o", type=str,
                        help="path to output CSV file (defaults to ~/data/<filename>)")
    parser.add_argument(
        "--filename",
        type=str,
        default="warehouse_data.csv",
        help="base name for generated file when --output is not given",
    )
    args = parser.parse_args()

    generate_warehouse_data(
        num_rows=args.rows,
        categories=args.categories,
        earliest_date=args.earliest,
        latest_date=args.latest,
        erroneous=args.erroneous,
        error_rate=args.error_rate,
        output_file=args.output,
        filename=args.filename,
    )


if __name__ == "__main__":
    _cli()
