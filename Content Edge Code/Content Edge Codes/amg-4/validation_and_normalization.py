import json
from datetime import datetime
from dateutil import parser
from normalise import clean_data

def is_valid_date(value):
    if not isinstance(value, str):
        return False
    try:
        parser.parse(value)
        return True
    except Exception:
        return False

def validate_row(row):
    errors = []

    if "article_date" not in row or not isinstance(row["article_date"], str) or not is_valid_date(row["article_date"]):
        errors.append("Invalid or missing article_date")

    if "article_title" not in row or not isinstance(row["article_title"], str) or not row["article_title"].strip():
        errors.append("Invalid or missing article_title")

    if "article_url" not in row or not isinstance(row["article_url"], str) or not row["article_url"].strip():
        errors.append("Invalid or missing article_url")

    return errors

def main(input_file):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    accepted = []
    rejected = []

    for row in data:
        errors = validate_row(row)

        if errors:
            rejected.append({
                "row": row,
                "reason": errors
            })
        else:

            cleaned_row = clean_data(row)
            accepted.append(cleaned_row)

    with open("accepted.json", "w", encoding="utf-8") as f:
        json.dump(accepted, f, indent=4, ensure_ascii=False)

    with open("rejected.json", "w", encoding="utf-8") as f:
        json.dump(rejected, f, indent=4, ensure_ascii=False)

    print(f"Accepted rows: {len(accepted)}")
    print(f"Rejected rows: {len(rejected)}")


if __name__ == "__main__":
    main(r"C:\tmp\am-432.json")

