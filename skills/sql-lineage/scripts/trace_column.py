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


def truncate_expr(expr: str | None, max_length: int | None) -> str | None:
    """Truncate expression to max_length if specified."""
    if expr is None or max_length is None or max_length <= 0:
        return expr
    if len(expr) <= max_length:
        return expr
    return expr[:max_length] + "..."


def node_to_dict(node: Node, depth: int = 0, max_expr_length: int | None = None) -> dict[str, Any]:
    """Convert a lineage Node to a dictionary representation."""
    expr_sql = node.expression.sql() if node.expression else None
    result = {
        "depth": depth,
        "name": node.name,
        "expression": truncate_expr(expr_sql, max_expr_length),
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


def extract_source_columns(expr: exp.Expression) -> list[str]:
    """Extract source column references from an expression (deduplicated)."""
    sources = set()
    for col in expr.find_all(exp.Column):
        table = col.table or "unknown"
        sources.add(f"{table}.{col.name}")
    return list(sources)


def find_column_in_union(ast: exp.Expression, column: str, max_expr_length: int | None = None) -> list[dict]:
    """Find column definitions across UNION branches."""
    locations = []
    
    # Check for UNION at top level
    for i, union in enumerate(ast.find_all(exp.Union)):
        # Left branch
        left = union.left
        if hasattr(left, 'selects'):
            for sel in left.selects:
                if sel.alias_or_name.lower() == column.lower():
                    locations.append({
                        "location": "union_branch",
                        "branch": f"left_{i+1}",
                        "expression": truncate_expr(sel.sql(), max_expr_length),
                        "sources": extract_source_columns(sel),
                    })
        
        # Right branch
        right = union.right
        if hasattr(right, 'selects'):
            for sel in right.selects:
                if sel.alias_or_name.lower() == column.lower():
                    locations.append({
                        "location": "union_branch",
                        "branch": f"right_{i+1}",
                        "expression": truncate_expr(sel.sql(), max_expr_length),
                        "sources": extract_source_columns(sel),
                    })
    
    return locations


def find_column_in_ctes(ast: exp.Expression, column: str, max_expr_length: int | None = None) -> list[dict]:
    """Find all CTEs where a column is defined."""
    locations = []
    for cte in ast.find_all(exp.CTE):
        cte_name = cte.alias
        
        # Check if CTE body is a UNION
        union_locs = find_column_in_union(cte.this, column, max_expr_length)
        if union_locs:
            for loc in union_locs:
                loc["cte_name"] = cte_name
                locations.append(loc)
        elif hasattr(cte.this, 'selects'):
            for sel in cte.this.selects:
                if sel.alias_or_name.lower() == column.lower():
                    locations.append({
                        "location": "cte",
                        "cte_name": cte_name,
                        "expression": truncate_expr(sel.sql(), max_expr_length),
                        "sources": extract_source_columns(sel),
                    })
    return locations


def build_cte_map(ast: exp.Expression) -> dict[str, exp.CTE]:
    """Build a map of CTE name -> CTE expression for fast lookup."""
    return {cte.alias.lower(): cte for cte in ast.find_all(exp.CTE)}


def find_column_in_cte(cte: exp.CTE, column: str, max_expr_length: int | None = None) -> dict | None:
    """Find a specific column definition within a single CTE."""
    if hasattr(cte.this, 'selects'):
        for sel in cte.this.selects:
            if sel.alias_or_name.lower() == column.lower():
                return {
                    "cte": cte.alias,
                    "column": sel.alias_or_name,
                    "expression": truncate_expr(sel.sql(), max_expr_length),
                    "sources": extract_source_columns(sel),
                }
    return None


def trace_cte_lineage_recursive(
    ast: exp.Expression,
    column: str,
    cte_map: dict[str, exp.CTE],
    max_expr_length: int | None = None,
    depth: int | None = None,
    visited: set | None = None,
) -> list[dict]:
    """
    Recursively trace a column through CTEs until we reach base tables.
    
    Returns a list representing the full lineage chain from output to source.
    """
    if visited is None:
        visited = set()
    
    # Normalize depth: treat 0 or negative as unlimited
    if depth is not None and depth <= 0:
        depth = None
    
    lineage_chain = []
    current_depth = 0
    
    # Queue of (table_or_cte, column) pairs to trace
    to_trace = [(None, column)]  # None means search all CTEs
    
    while to_trace:
        if depth is not None and current_depth >= depth:
            break
            
        next_to_trace = []
        
        for source_cte, col in to_trace:
            trace_key = f"{source_cte or 'root'}.{col}".lower()
            if trace_key in visited:
                continue
            visited.add(trace_key)
            
            if source_cte:
                # Look in specific CTE
                cte = cte_map.get(source_cte.lower())
                if cte:
                    col_info = find_column_in_cte(cte, col, max_expr_length)
                    if col_info:
                        lineage_chain.append(col_info)
                        # Queue up sources for next iteration
                        for src in col_info["sources"]:
                            parts = src.split(".")
                            if len(parts) == 2:
                                src_table, src_col = parts
                                if src_table.lower() in cte_map:
                                    next_to_trace.append((src_table, src_col))
                                elif src_table != "unknown":
                                    # It's a base table - add terminal node
                                    lineage_chain.append({
                                        "table": src_table,
                                        "column": src_col,
                                    })
            else:
                # Search all CTEs for initial column
                for cte_name, cte in cte_map.items():
                    col_info = find_column_in_cte(cte, col, max_expr_length)
                    if col_info:
                        lineage_chain.append(col_info)
                        for src in col_info["sources"]:
                            parts = src.split(".")
                            if len(parts) == 2:
                                src_table, src_col = parts
                                if src_table.lower() in cte_map:
                                    next_to_trace.append((src_table, src_col))
                                elif src_table != "unknown":
                                    lineage_chain.append({
                                        "table": src_table,
                                        "column": src_col,
                                    })
        
        to_trace = next_to_trace
        current_depth += 1
    
    return lineage_chain


def trace_column_lineage(
    sql: str,
    column: str,
    dialect: str | None = None,
    schema: dict | None = None,
    max_expr_length: int | None = None,
    depth: int | None = None,
) -> dict[str, Any]:
    """
    Trace a column's complete lineage through the query.

    Returns a dictionary with the column info and its full lineage chain.
    Now searches CTEs if column not in final output.
    """
    # Default to redshift dialect
    dialect = dialect or "redshift"

    # First, parse the AST to check column locations
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)
    except SqlglotError as e:
        return {
            "success": False,
            "column": column,
            "error": f"Parse error: {e}",
            "hint": "Check SQL syntax or try a different dialect",
        }

    # Check if column is in final SELECT
    in_final = False
    if hasattr(ast, 'selects'):
        in_final = any(s.alias_or_name.lower() == column.lower() for s in ast.selects)

    if in_final:
        # Use existing lineage() function for final output columns
        try:
            node = lineage(
                column,
                ast,  # Pass pre-parsed AST to avoid reparsing
                dialect=dialect,
                schema=schema or {},
            )

            # Walk the lineage tree and collect all nodes
            nodes_list = []
            edges_list = []
            node_to_index = {}

            # Use BFS to collect unique nodes and build edges
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
                nodes_list.append(node_to_dict(n, 0, max_expr_length))

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
                "in_final_output": True,
                "nodes": nodes_list,
                "edges": edges_list,
                "source_tables": list({
                    n["table"] for n in nodes_list
                    if n["type"] == "table"
                }),
            }

        except SqlglotError:
            # Fall through to CTE search if lineage() fails
            pass

    # Column not in final output (or lineage failed) - search CTEs
    cte_locations = find_column_in_ctes(ast, column, max_expr_length)

    if cte_locations:
        # Build CTE map for recursive tracing
        cte_map = build_cte_map(ast)
        
        # Perform recursive lineage tracing
        full_lineage = trace_cte_lineage_recursive(
            ast, column, cte_map, max_expr_length, depth
        )
        
        # Extract source tables (only base tables, not CTEs)
        source_tables = set()
        for item in full_lineage:
            if "table" in item and "cte" not in item:
                source_tables.add(item["table"])

        return {
            "success": True,
            "column": column,
            "in_final_output": False,
            "found_in": cte_locations,
            "full_lineage": full_lineage,
            "available_ctes": [cte.alias for cte in ast.find_all(exp.CTE)],
            "source_tables": list(source_tables),
            "note": f"Column '{column}' is defined in CTE(s), not in final SELECT output. Full lineage traced recursively.",
        }

    # Column truly not found anywhere
    # List available columns to help the agent
    available_columns = []
    if hasattr(ast, 'selects'):
        available_columns = [s.alias_or_name for s in ast.selects]

    # Also list CTE names to help
    cte_names = [cte.alias for cte in ast.find_all(exp.CTE)]

    return {
        "success": False,
        "column": column,
        "error": f"Column '{column}' not found in query",
        "available_in_output": available_columns[:10],
        "available_ctes": cte_names,
        "hint": "Check spelling. Use analyze_query.py to see all CTEs and their columns.",
    }


def format_output(result: dict, format_type: str) -> str:
    """Format the result based on requested format."""
    if format_type == "json":
        return json.dumps(result, indent=2)

    elif format_type == "tree":
        lines = [f"Column: {result['column']}", ""]
        if result.get("success"):
            # Check if this is a CTE-found result (no nodes/edges)
            if result.get("in_final_output") is False and "found_in" in result:
                lines.append("Note: Column not in final SELECT, found in CTE(s):\n")
                
                # Show full lineage chain if available
                full_lineage = result.get("full_lineage", [])
                if full_lineage:
                    lines.append("Full Lineage Chain:")
                    for i, item in enumerate(full_lineage):
                        indent = "  " * i
                        if "cte" in item:
                            lines.append(f"{indent}└── {item['cte']}.{item['column']}")
                            if item.get('expression'):
                                expr_preview = item['expression'][:100] + "..." if len(item.get('expression', '')) > 100 else item.get('expression', '')
                                lines.append(f"{indent}    Expression: {expr_preview}")
                        elif "table" in item:
                            lines.append(f"{indent}└── {item['table']}.{item['column']} (source table)")
                    lines.append("")
                
                # Also show initial findings for context
                lines.append("Initial CTE Definitions:")
                for loc in result.get("found_in", []):
                    lines.append(f"  CTE: {loc['cte_name']}")
                    if loc['sources']:
                        lines.append(f"    Sources: {', '.join(loc['sources'])}")
                    lines.append("")
            else:
                # Reconstruct tree via DFS for display (existing behavior)
                nodes = result.get("nodes", [])
                edges = result.get("edges", [])

                # Build adjacency list for children (reverse of edges: parent -> children)
                children_map = {i: [] for i in range(len(nodes))}
                for edge in edges:
                    # Edge is from child to parent. We want parent to child for tree view.
                    # edge["from"] is child, edge["to"] is parent
                    children_map[edge["to"]].append(edge["from"])

                # DFS helper
                def print_node(node_idx, depth):
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
            if result.get('available_in_output'):
                lines.append(f"Available columns: {', '.join(result['available_in_output'])}")
            if result.get('available_ctes'):
                lines.append(f"Available CTEs: {', '.join(result['available_ctes'])}")
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
        default="redshift",
        help="SQL dialect (default: redshift). Options: bigquery, snowflake, postgres, mysql, etc.",
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
    parser.add_argument(
        "--max-expr-length", "-m",
        type=int,
        default=None,
        help="Max characters for expression output (truncates with '...')",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Max depth for recursive CTE tracing (default: unlimited)",
    )

    args = parser.parse_args()

    sql = read_input(args.sql)
    schema = parse_schema(args.schema)

    result = trace_column_lineage(sql, args.column, args.dialect, schema, args.max_expr_length, args.depth)
    print(format_output(result, args.format))

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
