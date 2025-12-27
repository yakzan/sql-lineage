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
import html
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
        try:
            with open(value[1:], "r", encoding="utf-8") as f:
                return f.read()
        except FileNotFoundError:
            sys.exit(f"Error: File not found: {value[1:]}")
        except Exception as e:
            sys.exit(f"Error reading file {value[1:]}: {e}")
    return value


def parse_schema(schema_str: str | None) -> dict | None:
    """Parse schema from JSON string or file."""
    if not schema_str:
        return None
    content = read_input(schema_str)
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        sys.exit(f"Error: Invalid JSON schema: {e}")


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
        nodes_list = []
        edges_list = []
        node_to_index = {}
        
        # Use BFS to collect unique nodes and build edges
        # First pass: Collect all unique nodes
        unique_nodes = []
        visited = set()
        to_visit = [node]
        
        while to_visit:
            curr = to_visit.pop(0)
            if id(curr) not in visited:
                visited.add(id(curr))
                unique_nodes.append(curr)
                for child in curr.downstream:
                    to_visit.append(child)
        
        # Assign indices
        for i, n in enumerate(unique_nodes):
            node_to_index[id(n)] = i
            nodes_list.append(node_to_dict(n, 0))

        # Build edges
        for n in unique_nodes:
            parent_idx = node_to_index[id(n)]
            for child in n.downstream:
                if id(child) in node_to_index:
                    child_idx = node_to_index[id(child)]
                    edges_list.append({"from": child_idx, "to": parent_idx})

        return {
            "success": True,
            "column": column,
            "nodes": nodes_list,
            "edges": edges_list,
            "source_tables": list({
                n["table"] for n in nodes_list
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
            # Reconstruct tree via DFS for display
            nodes = result["nodes"]
            edges = result["edges"]
            
            # Build adjacency list for children (reverse of edges: parent -> children)
            children_map = {i: [] for i in range(len(nodes))}
            for edge in edges:
                # Edge is from child to parent. We want parent to child for tree view.
                # edge["from"] is child, edge["to"] is parent
                children_map[edge["to"]].append(edge["from"])
            
            # DFS helper
            def print_node(node_idx, depth, prefix=""):
                node = nodes[node_idx]
                indent = "  " * depth
                
                # Format node string
                if node["type"] == "table":
                    content = f"└── {node['table']}.{node['column']} (source table)"
                else:
                    content = f"└── {node['expression']} ({node['type']})"
                
                lines.append(f"{indent}{content}")
                
                for child_idx in children_map.get(node_idx, []):
                    print_node(child_idx, depth + 1)

            # Start from root (index 0)
            if nodes:
                print_node(0, 0)
                
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

    for i, node in enumerate(result["nodes"]):
        label_text = f"{node['table']}.{node['column']}" if node["type"] == "table" else node["expression"]
        # Safe JSON serialization for label to prevent XSS and handle special chars
        label = json.dumps(label_text or "UNKNOWN")
        color = "#97C2FC" if node["type"] == "table" else "#FB7E81"
        nodes_js.append(f'{{id: {i}, label: {label}, color: "{color}"}}')

    for edge in result["edges"]:
        edges_js.append(f'{{from: {edge["from"]}, to: {edge["to"]}, arrows: "to"}}')

    # Escape HTML content to prevent XSS
    escaped_column = html.escape(result['column'])
    escaped_tables = html.escape(', '.join(result['source_tables']))

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Column Lineage: {escaped_column}</title>
    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        #graph {{ width: 100%; height: 600px; border: 1px solid #ccc; }}
        body {{ font-family: Arial, sans-serif; padding: 20px; }}
    </style>
</head>
<body>
    <h1>Lineage for column: {escaped_column}</h1>
    <p>Source tables: {escaped_tables}</p>
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
