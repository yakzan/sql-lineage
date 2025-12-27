#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
Trace a column's lineage back to its source tables and columns.

Usage:
    uv run trace_column.py "SELECT a FROM (SELECT x AS a FROM t)" --column a
    uv run trace_column.py @query.sql --column user_id --dialect bigquery
    uv run trace_column.py "SELECT * FROM x" --column id --schema '{"x": {"id": "INT"}}'
"""

import argparse
import json
import sys
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage, Node


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


def node_to_dict(node: Node, depth: int = 0) -> dict[str, Any]:
    """Convert a lineage Node to a dictionary representation."""
    result = {
        "depth": depth,
        "name": node.name,
        "expression": node.expression.sql() if node.expression else None,
    }

    if isinstance(node.expression, exp.Table):
        result["type"] = "table"
        result["table"] = node.expression.name
        result["column"] = node.name
    elif isinstance(node.expression, exp.Subquery):
        result["type"] = "subquery"
    else:
        result["type"] = "derived"

    return result


def trace_column_lineage(
    sql: str,
    column: str,
    dialect: str | None = None,
    schema: dict | None = None,
) -> dict[str, Any]:
    """
    Trace a column's complete lineage through the query.

    Returns a dictionary with the column info and its full lineage chain.
    """
    try:
        # Get the lineage node for the column
        node = lineage(
            column,
            sql,
            dialect=dialect,
            schema=schema or {},
        )

        # Walk the lineage tree and collect all nodes
        lineage_chain = []
        for depth, n in enumerate(node.walk()):
            lineage_chain.append(node_to_dict(n, depth))

        return {
            "success": True,
            "column": column,
            "lineage": lineage_chain,
            "source_tables": list({
                n["table"] for n in lineage_chain
                if n["type"] == "table"
            }),
        }

    except SqlglotError as e:
        return {
            "success": False,
            "column": column,
            "error": str(e),
            "hint": get_error_hint(str(e)),
        }


def get_error_hint(error: str) -> str:
    """Provide helpful hints for common errors."""
    if "Cannot find column" in error:
        return "Check that the column name matches exactly (case-sensitive) and is in the SELECT list."
    if "Cannot build lineage" in error:
        return "Ensure the SQL is a SELECT statement. CREATE/INSERT not supported for lineage."
    if "schema" in error.lower():
        return "Try providing a schema with --schema for queries with SELECT * or ambiguous columns."
    return "Check SQL syntax and dialect setting."


def format_output(result: dict, format_type: str) -> str:
    """Format the result based on requested format."""
    if format_type == "json":
        return json.dumps(result, indent=2)

    elif format_type == "tree":
        lines = [f"Column: {result['column']}", ""]
        if result.get("success"):
            for node in result["lineage"]:
                indent = "  " * node["depth"]
                if node["type"] == "table":
                    lines.append(f"{indent}└── {node['table']}.{node['column']} (source table)")
                else:
                    lines.append(f"{indent}└── {node['expression']} ({node['type']})")
        else:
            lines.append(f"Error: {result['error']}")
            lines.append(f"Hint: {result.get('hint', '')}")
        return "\n".join(lines)

    elif format_type == "html":
        # Generate an HTML visualization using vis.js
        return generate_html_visualization(result)

    return json.dumps(result, indent=2)


def generate_html_visualization(result: dict) -> str:
    """Generate an HTML page with interactive lineage visualization."""
    if not result.get("success"):
        return f"<html><body><h1>Error</h1><p>{result.get('error')}</p></body></html>"

    nodes_js = []
    edges_js = []

    for i, node in enumerate(result["lineage"]):
        label = f"{node['table']}.{node['column']}" if node["type"] == "table" else node["expression"]
        color = "#97C2FC" if node["type"] == "table" else "#FB7E81"
        nodes_js.append(f'{{id: {i}, label: "{label}", color: "{color}"}}')

        if i > 0:
            edges_js.append(f'{{from: {i}, to: {i-1}}}')

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Column Lineage: {result['column']}</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        #graph {{ width: 100%; height: 600px; border: 1px solid #ccc; }}
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
    </style>
</head>
<body>
    <h1>Lineage for column: {result['column']}</h1>
    <p>Source tables: {', '.join(result['source_tables'])}</p>
    <div id="graph"></div>
    <script>
        var nodes = new vis.DataSet([{', '.join(nodes_js)}]);
        var edges = new vis.DataSet([{', '.join(edges_js)}]);
        var container = document.getElementById('graph');
        var data = {{ nodes: nodes, edges: edges }};
        var options = {{ layout: {{ hierarchical: {{ direction: 'UD' }} }} }};
        new vis.Network(container, data, options);
    </script>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(
        description="Trace a column's lineage using sqlglot AST analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  uv run trace_column.py "SELECT id FROM users" --column id

  # From file with dialect
  uv run trace_column.py @query.sql --column user_id --dialect bigquery

  # With schema for SELECT *
  uv run trace_column.py "SELECT * FROM t" -c id -s '{"t": {"id": "INT", "name": "VARCHAR"}}'

  # HTML visualization output
  uv run trace_column.py "SELECT ..." -c col -f html > lineage.html
        """,
    )

    parser.add_argument(
        "sql",
        help="SQL query string, or @filepath to read from file",
    )
    parser.add_argument(
        "--column", "-c",
        required=True,
        help="Column name to trace",
    )
    parser.add_argument(
        "--dialect", "-d",
        default=None,
        help="SQL dialect (bigquery, snowflake, postgres, mysql, etc.)",
    )
    parser.add_argument(
        "--schema", "-s",
        default=None,
        help="JSON schema string or @filepath for disambiguation",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "tree", "html"],
        default="json",
        help="Output format (default: json)",
    )

    args = parser.parse_args()

    sql = read_input(args.sql)
    schema = parse_schema(args.schema)

    result = trace_column_lineage(sql, args.column, args.dialect, schema)
    print(format_output(result, args.format))

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
