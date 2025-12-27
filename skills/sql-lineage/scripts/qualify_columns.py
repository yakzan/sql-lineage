#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
Qualify all column references in a SQL query with their table names.

Usage:
    uv run qualify_columns.py "SELECT id, name FROM users" --schema '{"users": {"id": "INT", "name": "VARCHAR"}}'
"""

import argparse
import json
import sys

import sqlglot
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify


def read_input(value: str) -> str:
    if value.startswith("@"):
        try:
            with open(value[1:], "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            sys.exit(f"Error: File not found: {value[1:]}")
        except Exception as e:
            sys.exit(f"Error reading file {value[1:]}: {e}")
    return value


def parse_schema(schema_str: str | None) -> dict | None:
    if not schema_str:
        return None
    content = read_input(schema_str)
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Invalid JSON schema: {e}")


def qualify_query(sql: str, dialect: str | None = None, schema: dict | None = None) -> dict:
    """Qualify all column references in a query."""
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)

        qualified = qualify(
            ast,
            dialect=dialect,
            schema=schema or {},
            validate_qualify_columns=False,
            identify=False,
        )

        return {
            "success": True,
            "original": sql,
            "qualified": qualified.sql(dialect=dialect, pretty=True),
        }

    except SqlglotError as e:
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Qualify column references in SQL")
    parser.add_argument("sql", help="SQL query or @filepath")
    parser.add_argument("--dialect", "-d", default=None)
    parser.add_argument("--schema", "-s", default=None, help="JSON schema")
    parser.add_argument("--sql-only", action="store_true", help="Output only the qualified SQL")

    args = parser.parse_args()
    sql = read_input(args.sql)
    schema = parse_schema(args.schema)

    result = qualify_query(sql, args.dialect, schema)

    if args.sql_only and result.get("success"):
        print(result["qualified"])
    else:
        print(json.dumps(result, indent=2))

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
