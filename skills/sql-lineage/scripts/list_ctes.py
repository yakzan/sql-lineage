#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
List all CTEs in a SQL query with their output columns.

Usage:
    uv run list_ctes.py "WITH a AS (SELECT 1 AS x) SELECT * FROM a"
    uv run list_ctes.py @query.sql --dialect redshift
    uv run list_ctes.py @query.sql --format json
"""

import argparse
import json
import sys

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError


def read_input(value: str) -> str:
    """Read from file if value starts with @, otherwise return as-is."""
    if value.startswith("@"):
        try:
            with open(value[1:], "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            sys.exit(f"Error: File not found: {value[1:]}")
        except Exception as e:
            sys.exit(f"Error reading file {value[1:]}: {e}")
    return value


def extract_cte_info(cte: exp.CTE) -> dict:
    """Extract information about a single CTE."""
    columns = []
    if hasattr(cte.this, 'selects'):
        for sel in cte.this.selects:
            columns.append(sel.alias_or_name)
    
    # Extract tables referenced in this CTE
    tables = []
    for table in cte.this.find_all(exp.Table):
        table_name = table.name
        if table_name and table_name not in tables:
            tables.append(table_name)
    
    return {
        "name": cte.alias,
        "columns": columns,
        "references": tables,
    }


def list_ctes(sql: str, dialect: str | None = None) -> dict:
    """
    List all CTEs in a SQL query with their columns.
    
    Returns a dictionary with CTE information.
    """
    dialect = dialect or "redshift"
    
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)
    except SqlglotError as e:
        return {
            "success": False,
            "error": f"Parse error: {e}",
            "hint": "Check SQL syntax or try a different dialect",
        }
    
    ctes = []
    for cte in ast.find_all(exp.CTE):
        ctes.append(extract_cte_info(cte))
    
    # Also get final output columns
    final_columns = []
    if hasattr(ast, 'selects'):
        final_columns = [s.alias_or_name for s in ast.selects]
    
    return {
        "success": True,
        "cte_count": len(ctes),
        "ctes": ctes,
        "final_output_columns": final_columns,
    }


def format_text(result: dict) -> str:
    """Format result as human-readable text."""
    if not result.get("success"):
        return f"Error: {result.get('error')}\nHint: {result.get('hint', '')}"
    
    lines = [f"Found {result['cte_count']} CTE(s):", ""]
    
    for cte in result["ctes"]:
        lines.append(f"CTE: {cte['name']}")
        if cte["columns"]:
            cols_str = ", ".join(cte["columns"][:20])
            if len(cte["columns"]) > 20:
                cols_str += f" ... (+{len(cte['columns']) - 20} more)"
            lines.append(f"  Columns: {cols_str}")
        if cte["references"]:
            lines.append(f"  References: {', '.join(cte['references'])}")
        lines.append("")
    
    if result["final_output_columns"]:
        cols_str = ", ".join(result["final_output_columns"][:20])
        if len(result["final_output_columns"]) > 20:
            cols_str += f" ... (+{len(result['final_output_columns']) - 20} more)"
        lines.append(f"Final Output Columns: {cols_str}")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="List all CTEs in a SQL query with their output columns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  uv run list_ctes.py "WITH a AS (SELECT 1 AS x) SELECT * FROM a"

  # From file
  uv run list_ctes.py @query.sql

  # JSON output
  uv run list_ctes.py @query.sql --format json
        """,
    )
    
    parser.add_argument(
        "sql",
        help="SQL query string, or @filepath to read from file",
    )
    parser.add_argument(
        "--dialect", "-d",
        default="redshift",
        help="SQL dialect (default: redshift)",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    
    args = parser.parse_args()
    
    sql = read_input(args.sql)
    result = list_ctes(sql, args.dialect)
    
    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        print(format_text(result))
    
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
