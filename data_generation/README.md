# Warehouse Data Generator

This document describes the `generate_warehouse_data.py` utility and the
`generate_warehouse_data` function it exposes.  The module can be used as a
command line script or imported into Python code for programmatic data
creation.

---

## API

```python
from data_generation.generate_warehouse_data import generate_warehouse_data
```

### `generate_warehouse_data` signature

```python
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
```

#### Parameters

- **num_rows**: Number of rows to generate.  Defaults to `2000`.
- **categories**: Iterable of category names to sample from.  If omitted a
  sensible list ("Computers&Accessories", "Electronics", "HomeGoods",
  "Clothing", "Sports", "Books") is used.
- **earliest_date** / **latest_date**: Bounds for order dates.  Accepts either
  `datetime.date` objects or ISO formatted strings ("YYYY-MM-DD").  Defaults
  to one year ago and today, respectively.
- **erroneous**: When `True` random errors will be injected (misspellings,
  negative prices, malformed/missing values, column shifts, etc).  Default is
  `False`.
- **error_rate**: Probability that any particular cell will be corrupted when
  `erroneous` is `True`.  Defaults to `0.001` (0.1%).  The argument is ignored
  when `erroneous` is `False`.
- **output_file**: Full path where CSV output should be written.  If
  unspecified the function writes to `~/data/<filename>` (see below).
- **filename**: Base name used when `output_file` is not provided.  Defaults
  to `warehouse_data.csv`.

#### Return value

List of dictionaries, one per generated row.  Keys correspond to columns:
`ItemHash`, `SkuNumber`, `SoldFlag`, `ItemName`, `Price`, `WarehouseId`,
`Category`, `OrderDate`, `ShipDate`, `OutDate`, `Delivery_date`, `OrderId`,
`Quantity`, `CustomerId`.

---

## Command Line Interface

Run the module directly with Python:

```bash
python data_generation/generate_warehouse_data.py [options]
```

### Options

- `-n`, `--rows <int>` – number of rows (default 2000)
- `-c`, `--categories <str>...` – space-separated list of categories
- `--earliest <YYYY-MM-DD>` – earliest order date
- `--latest <YYYY-MM-DD>` – latest order date
- `--erroneous` – include random errors
- `--error-rate <float>` – per-cell error probability (default 0.001)
- `--output`, `-o <path>` – full output CSV path
- `--filename <name>` – base filename when `--output` is omitted

Examples:

```bash
# 500 rows, default categories, clean data
python data_generation/generate_warehouse_data.py -n 500

# custom categories and date range
python data_generation/generate_warehouse_data.py -c Toys Books --earliest 2024-01-01 --latest 2024-12-31

# erroneous data with custom filename
python data_generation/generate_warehouse_data.py --erroneous --filename bad.csv
```

The script will write a CSV either to the supplied `--output` path or to
`~/data/<filename>` by default.  The directory is created if it does not
already exist.

---

## Error Injection Details

When `erroneous=True` the following types of errors may occur:

1. **Horizontal shifts** – values from adjacent columns may appear in place of
   the correct one (e.g. price appearing in warehouse ID).
2. **Vertical shifts** – values may be swapped between rows or moved from one
   row to another, leaving a `NULL` behind.
3. **Value corruption** – negative numbers for prices, malformed dates, string
   garbling, etc.

The `error_rate` controls how often any given cell is affected.  A secondary
chance influences whether a shift or a simple corruption is used.

---

## Extending and Embedding

The code is designed to be simple to extend.  To add more columns, insert new
entries into the `record` dict inside the row generation loop; the error
injection logic is generic and will automatically operate on any added keys.

Feel free to import `generate_warehouse_data` into unit tests, notebooks or
other data pipelines.
