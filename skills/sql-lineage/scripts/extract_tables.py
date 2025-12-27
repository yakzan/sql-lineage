#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
Extract all tables referenced in a SQL query.

Usage:
    uv run extract_tables.py "SELECT * FROM a JOIN b ON a.id = b.id"
"""

import argparse
import json
import sys

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError


def read_input(value: str) -> str:
    if value.startswith("@"):
        with open(value[1:], "r") as f:
            return f.read()
    return value


def extract_tables(sql: str, dialect: str | None = None) -> list[dict]:
    """Extract all table references from SQL."""
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)
        tables = []

        for table in ast.find_all(exp.Table):
            table_info = {
                "name": table.name,
                "database": table.db if table.db else None,
                "catalog": table.catalog if table.catalog else None,
                "alias": table.alias if table.alias else None,
            }
            # Construct fully qualified name
            parts = [p for p in [table.catalog, table.db, table.name] if p]
            table_info["qualified_name"] = ".".join(parts)
            tables.append(table_info)

        return {"success": True, "tables": tables}

    except SqlglotError as e:
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Extract tables from SQL")
    parser.add_argument("sql", help="SQL query or @filepath")
    parser.add_argument("--dialect", "-d", default=None)
    parser.add_argument("--names-only", action="store_true", help="Output only table names")

    args = parser.parse_args()
    sql = read_input(args.sql)
    result = extract_tables(sql, args.dialect)

    if args.names_only and result.get("success"):
        names = list(set(t["name"] for t in result["tables"]))
        print("\n".join(names))
    else:
        print(json.dumps(result, indent=2))

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
