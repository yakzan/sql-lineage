#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
Analyze a SQL query to extract all columns, tables, joins, and transformations.

Usage:
    uv run analyze_query.py "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
    uv run analyze_query.py @query.sql --dialect snowflake --format markdown
"""

import argparse
import json
import sys
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import build_scope, find_all_in_scope, traverse_scope


def read_input(value: str) -> str:
    """Read from file if value starts with @, otherwise return as-is."""
    if value.startswith("@"):
        with open(value[1:], "r") as f:
            return f.read()
    return value


def parse_schema(schema_str: str | None) -> dict | None:
    """Parse schema from JSON string or file."""
    if not schema_str:
        return None
    content = read_input(schema_str)
    return json.loads(content)


def classify_transformation(select_expr: exp.Expression) -> str:
    """Classify the type of transformation applied to a column."""
    if isinstance(select_expr, exp.Column):
        return "passthrough"

    if isinstance(select_expr, exp.Alias):
        inner = select_expr.this
        if isinstance(inner, exp.Column):
            return "renamed"
        if isinstance(inner, (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)):
            return "aggregated"
        if isinstance(inner, exp.Window):
            return "window_function"
        return "derived"

    if isinstance(select_expr, (exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)):
        return "aggregated"

    if isinstance(select_expr, exp.Window):
        return "window_function"

    return "derived"


def extract_source_columns(expr: exp.Expression) -> list[dict]:
    """Extract all source column references from an expression."""
    sources = []
    for col in expr.find_all(exp.Column):
        sources.append({
            "table": col.table or "unknown",
            "column": col.name,
        })
    return sources


def analyze_select(ast: exp.Expression, dialect: str | None, schema: dict | None) -> dict:
    """Analyze a SELECT statement."""
    result = {
        "query_type": "SELECT",
        "dialect": dialect,
        "tables": [],
        "ctes": [],
        "columns": [],
        "joins": [],
        "filters": [],
        "group_by": [],
        "order_by": [],
        "aggregations": [],
        "window_functions": [],
    }

    # Try to qualify columns for better analysis
    try:
        if schema:
            qualified = qualify(ast, dialect=dialect, schema=schema)
        else:
            qualified = qualify(ast, dialect=dialect, validate_qualify_columns=False)
    except SqlglotError:
        qualified = ast

    # Extract tables
    for table in qualified.find_all(exp.Table):
        table_info = {
            "name": table.name,
            "alias": table.alias if table.alias else None,
            "schema": table.db if table.db else None,
        }
        if table_info not in result["tables"]:
            result["tables"].append(table_info)

    # Extract CTEs
    for cte in qualified.find_all(exp.CTE):
        result["ctes"].append({
            "name": cte.alias,
            "columns": [col.alias_or_name for col in cte.this.selects] if hasattr(cte.this, 'selects') else [],
        })

    # Analyze SELECT columns
    if hasattr(qualified, 'selects'):
        for i, select_expr in enumerate(qualified.selects):
            col_info = {
                "output_position": i + 1,
                "output_name": select_expr.alias_or_name,
                "expression": select_expr.sql(dialect=dialect),
                "transformation": classify_transformation(select_expr),
                "sources": extract_source_columns(select_expr),
            }
            result["columns"].append(col_info)

    # Extract JOINs
    for join in qualified.find_all(exp.Join):
        join_info = {
            "type": join.kind or "INNER",
            "table": join.this.name if isinstance(join.this, exp.Table) else str(join.this),
            "condition": join.args.get("on").sql(dialect=dialect) if join.args.get("on") else None,
        }
        result["joins"].append(join_info)

    # Extract WHERE filters
    where = qualified.find(exp.Where)
    if where:
        result["filters"].append(where.this.sql(dialect=dialect))

    # Extract GROUP BY
    group = qualified.find(exp.Group)
    if group:
        for expr in group.expressions:
            result["group_by"].append(expr.sql(dialect=dialect))

    # Extract ORDER BY
    order = qualified.find(exp.Order)
    if order:
        for expr in order.expressions:
            result["order_by"].append(expr.sql(dialect=dialect))

    # Identify aggregations and window functions
    for agg in qualified.find_all((exp.Sum, exp.Avg, exp.Count, exp.Min, exp.Max)):
        result["aggregations"].append(agg.sql(dialect=dialect))

    for win in qualified.find_all(exp.Window):
        result["window_functions"].append(win.sql(dialect=dialect))

    return result


def analyze_query(
    sql: str,
    dialect: str | None = None,
    schema: dict | None = None,
) -> dict[str, Any]:
    """
    Perform full analysis of a SQL query.

    Returns comprehensive information about tables, columns, joins, and transformations.
    """
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)

        if isinstance(ast, exp.Select):
            return {"success": True, **analyze_select(ast, dialect, schema)}

        elif isinstance(ast, exp.Create):
            # Handle CREATE TABLE ... AS SELECT
            if ast.expression and isinstance(ast.expression, exp.Select):
                result = analyze_select(ast.expression, dialect, schema)
                result["query_type"] = "CREATE_TABLE_AS_SELECT"
                result["target_table"] = ast.this.name if ast.this else None
                return {"success": True, **result}
            return {
                "success": True,
                "query_type": "CREATE",
                "target": ast.this.name if ast.this else None,
            }

        elif isinstance(ast, exp.Insert):
            result = {"query_type": "INSERT", "target_table": None}
            if ast.this:
                result["target_table"] = ast.this.name
            if ast.expression and isinstance(ast.expression, exp.Select):
                select_analysis = analyze_select(ast.expression, dialect, schema)
                result.update(select_analysis)
                result["query_type"] = "INSERT_SELECT"
            return {"success": True, **result}

        else:
            return {
                "success": True,
                "query_type": type(ast).__name__.upper(),
                "sql": ast.sql(dialect=dialect),
            }

    except SqlglotError as e:
        return {
            "success": False,
            "error": str(e),
            "hint": "Check SQL syntax and dialect setting.",
        }


def format_as_markdown(result: dict) -> str:
    """Format the analysis result as Markdown."""
    lines = [f"# SQL Analysis\n"]
    lines.append(f"**Query Type:** {result.get('query_type', 'Unknown')}\n")

    if result.get("tables"):
        lines.append("## Tables\n")
        for t in result["tables"]:
            alias = f" (alias: {t['alias']})" if t.get("alias") else ""
            lines.append(f"- `{t['name']}`{alias}")
        lines.append("")

    if result.get("ctes"):
        lines.append("## CTEs (Common Table Expressions)\n")
        for cte in result["ctes"]:
            lines.append(f"- **{cte['name']}**: {', '.join(cte.get('columns', []))}")
        lines.append("")

    if result.get("columns"):
        lines.append("## Output Columns\n")
        lines.append("| # | Name | Transformation | Sources | Expression |")
        lines.append("|---|------|----------------|---------|------------|")
        for col in result["columns"]:
            sources = ", ".join(f"{s['table']}.{s['column']}" for s in col.get("sources", []))
            expr = col.get("expression", "")[:50]
            lines.append(f"| {col['output_position']} | {col['output_name']} | {col['transformation']} | {sources} | `{expr}` |")
        lines.append("")

    if result.get("joins"):
        lines.append("## Joins\n")
        for j in result["joins"]:
            lines.append(f"- **{j['type']} JOIN** `{j['table']}` ON `{j.get('condition', 'N/A')}`")
        lines.append("")

    if result.get("filters"):
        lines.append("## Filters (WHERE)\n")
        for f in result["filters"]:
            lines.append(f"- `{f}`")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze a SQL query to extract structure and column information",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "sql",
        help="SQL query string, or @filepath to read from file",
    )
    parser.add_argument(
        "--dialect", "-d",
        default=None,
        help="SQL dialect (bigquery, snowflake, postgres, mysql, etc.)",
    )
    parser.add_argument(
        "--schema", "-s",
        default=None,
        help="JSON schema string or @filepath",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "markdown"],
        default="json",
        help="Output format (default: json)",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (default: stdout)",
    )

    args = parser.parse_args()

    sql = read_input(args.sql)
    schema = parse_schema(args.schema)

    result = analyze_query(sql, args.dialect, schema)

    if args.format == "markdown":
        output = format_as_markdown(result)
    else:
        output = json.dumps(result, indent=2)

    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Analysis written to {args.output}")
    else:
        print(output)

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
