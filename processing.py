"""
processing.py

pure and isolated dataset processing logic.
takes a parsed JSON dict, returns a result dict.

"""

REQUIRED_FIELDS = {"id", "timestamp", "value", "category"}


def is_valid_record(record: dict) -> bool:
    """
    a record is valid if:
      - it contains all required fields (id, timestamp, value, category)
      - the 'value' field is numeric (int or float, but not bool)
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
    process a parsed dataset JSON and return a result summary.

    expected input shape and return value in accordance to pdf.

    raises:
        ValueError if the top-level structure is malformed (missing dataset_id or records).

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
        # records must be dicts
        if not isinstance(record, dict):
            invalid_count += 1
            continue

        if not is_valid_record(record):
            invalid_count += 1
            continue

        # stat accumulation for a valid record
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