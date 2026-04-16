"""
processing.py

Pure dataset processing logic. No database, no HTTP, no async.
Takes a parsed JSON dict, returns a result dict.

Keeping this isolated means it can be unit-tested independently
and reused without any infrastructure knowledge.
"""

REQUIRED_FIELDS = {"id", "timestamp", "value", "category"}


def is_valid_record(record: dict) -> bool:
    """
    A record is valid if:
      - It contains all required fields (id, timestamp, value, category)
      - The 'value' field is numeric (int or float, but not bool)

    We explicitly reject booleans because in Python, bool is a subclass
    of int — so isinstance(True, int) is True. That's not what we want.
    """
    if not REQUIRED_FIELDS.issubset(record.keys()):
        return False

    value = record["value"]
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False

    return True


def process_dataset(data: dict) -> dict:
    """
    Process a parsed dataset JSON and return a result summary.

    Expected input shape:
        {
            "dataset_id": "ds_001",
            "records": [ { "id": ..., "timestamp": ..., "value": ..., "category": ... }, ... ]
        }

    Returns:
        {
            "dataset_id": str,
            "record_count": int,        # all records, including invalid
            "category_summary": dict,   # counts per category, valid records only
            "average_value": float,     # mean of value across valid records (None if none)
            "invalid_records": int
        }

    Raises:
        ValueError if the top-level structure is malformed (missing dataset_id or records).
        We let this propagate so the worker can catch it and mark the task FAILED.
    """
    if "dataset_id" not in data:
        raise ValueError("Missing required field: 'dataset_id'")

    if "records" not in data or not isinstance(data["records"], list):
        raise ValueError("Missing or malformed field: 'records' must be a list")

    dataset_id = data["dataset_id"]
    records = data["records"]

    record_count = len(records)
    invalid_count = 0
    category_summary: dict[str, int] = {}
    value_sum = 0.0
    valid_count = 0

    for record in records:
        # Records must be dicts — anything else (a string, a number, etc.) is invalid
        if not isinstance(record, dict):
            invalid_count += 1
            continue

        if not is_valid_record(record):
            invalid_count += 1
            continue

        # Valid record — accumulate stats
        category = record["category"]
        value = record["value"]

        category_summary[category] = category_summary.get(category, 0) + 1
        value_sum += value
        valid_count += 1

    average_value = (value_sum / valid_count) if valid_count > 0 else None

    return {
        "dataset_id": dataset_id,
        "record_count": record_count,
        "category_summary": category_summary,
        "average_value": average_value,
        "invalid_records": invalid_count,
    }