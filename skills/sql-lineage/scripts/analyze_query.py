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


def extract_aggregation_info(select_expr: exp.Expression) -> dict | None:
    """
    Extract aggregation semantics from an expression.

    Returns a dict with aggregation details if the expression is aggregated, None otherwise.
    """
    # Unwrap Alias
    expr = select_expr.this if isinstance(select_expr, exp.Alias) else select_expr

    agg_funcs = {
        exp.Sum: "SUM",
        exp.Avg: "AVG",
        exp.Count: "COUNT",
        exp.Min: "MIN",
        exp.Max: "MAX",
    }

    # Check if the expression IS an aggregation function
    for agg_type, agg_name in agg_funcs.items():
        if isinstance(expr, agg_type):
            # Extract what's being aggregated
            agg_input = []
            for col in expr.find_all(exp.Column):
                agg_input.append(f"{col.table or ''}.{col.name}".lstrip("."))
            return {
                "function": agg_name,
                "input_columns": agg_input if agg_input else ["*"] if agg_name == "COUNT" else [],
            }

    # Check if expression CONTAINS aggregation functions (derived aggregation)
    for agg_type, agg_name in agg_funcs.items():
        found_aggs = list(expr.find_all(agg_type))
        if found_aggs:
            # Collect all aggregations in the expression
            aggs = []
            for agg in found_aggs:
                agg_input = []
                for col in agg.find_all(exp.Column):
                    agg_input.append(f"{col.table or ''}.{col.name}".lstrip("."))
                agg_func_name = agg_funcs.get(type(agg), "UNKNOWN")
                aggs.append({
                    "function": agg_func_name,
                    "input_columns": agg_input if agg_input else ["*"] if agg_func_name == "COUNT" else [],
                })
            return {
                "function": "DERIVED",
                "contains": aggs,
            }

    return None


def infer_data_type(select_expr: exp.Expression, schema: dict | None = None) -> str:
    """
    Infer the data type of an expression.

    Returns a string describing the inferred type.
    """
    # Unwrap Alias to get the actual expression
    expr = select_expr.this if isinstance(select_expr, exp.Alias) else select_expr

    # Check for CAST - explicit type conversion
    if isinstance(expr, exp.Cast):
        if expr.to and hasattr(expr.to, 'this'):
            return str(expr.to.this).upper()
        return "UNKNOWN"

    # Aggregation functions have known return types
    if isinstance(expr, exp.Count):
        return "BIGINT"
    if isinstance(expr, exp.Sum):
        return "NUMERIC"
    if isinstance(expr, exp.Avg):
        return "DOUBLE"
    if isinstance(expr, (exp.Min, exp.Max)):
        return "INHERITED"  # Same type as input

    # Window functions - try to infer from the inner function
    if isinstance(expr, exp.Window):
        inner_func = expr.this
        if isinstance(inner_func, exp.Count):
            return "BIGINT"
        if isinstance(inner_func, exp.Sum):
            return "NUMERIC"
        if isinstance(inner_func, exp.Avg):
            return "DOUBLE"
        if isinstance(inner_func, (exp.RowNumber, exp.Rank, exp.DenseRank)):
            return "BIGINT"
        return "INHERITED"

    # Boolean expressions
    if isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
                         exp.And, exp.Or, exp.Not, exp.In, exp.Between,
                         exp.Is, exp.Like)):
        return "BOOLEAN"

    # String functions
    if isinstance(expr, (exp.Concat, exp.Substring, exp.Upper, exp.Lower,
                         exp.Trim, exp.Replace)):
        return "VARCHAR"

    # Date/time functions
    if isinstance(expr, (exp.CurrentDate, exp.CurrentTimestamp)):
        return "TIMESTAMP"
    if isinstance(expr, exp.DateTrunc):
        return "TIMESTAMP"
    if isinstance(expr, exp.Extract):
        return "INTEGER"
    if isinstance(expr, exp.DateDiff):
        return "INTEGER"

    # Arithmetic - preserve numeric type
    if isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
        return "NUMERIC"

    # CASE expression - check THEN clauses for type hints
    if isinstance(expr, exp.Case):
        # Look at the first THEN clause for type hint
        for when in expr.args.get("ifs", []):
            then_expr = when.args.get("true")
            if then_expr:
                if isinstance(then_expr, exp.Literal):
                    if then_expr.is_string:
                        return "VARCHAR"
                    if then_expr.is_number:
                        return "NUMERIC"
        return "CONDITIONAL"

    # Simple column reference - try to get from schema
    if isinstance(expr, exp.Column):
        if schema:
            table = expr.table or ""
            col_name = expr.name
            # Try to find in schema
            for tbl_name, columns in schema.items():
                if tbl_name.lower() == table.lower() or not table:
                    if col_name in columns:
                        return str(columns[col_name]).upper()
        return "UNKNOWN"

    # Literal values
    if isinstance(expr, exp.Literal):
        if expr.is_string:
            return "VARCHAR"
        if expr.is_number:
            if "." in str(expr.this):
                return "DECIMAL"
            return "INTEGER"

    # Coalesce/NVL - inherit from first non-null argument
    # Note: NVL is usually parsed as Coalesce or a function call by sqlglot
    if isinstance(expr, exp.Coalesce):
        return "INHERITED"

    return "UNKNOWN"


def build_alias_map(selects: list[exp.Expression]) -> dict[str, exp.Expression]:
    """
    Build a map of column aliases to their expressions from a SELECT list.

    This enables resolution of self-referencing columns.
    """
    alias_map = {}
    for sel in selects:
        alias_name = sel.alias_or_name.lower()
        if isinstance(sel, exp.Alias):
            alias_map[alias_name] = sel.this
        else:
            alias_map[alias_name] = sel
    return alias_map


def extract_source_columns(
    expr: exp.Expression,
    alias_map: dict[str, exp.Expression] | None = None,
    _visited: set[str] | None = None,
) -> list[dict]:
    """Extract all source column references from an expression."""
    sources = []
    alias_map = alias_map or {}
    seen = set()
    visited = _visited if _visited is not None else set()

    for col in expr.find_all(exp.Column):
        table = col.table or "unknown"
        col_name = col.name.lower()

        # Self-referencing resolution
        if table == "unknown" and col_name in alias_map and col_name not in visited:
            visited.add(col_name)
            # Recursively extract sources from the referenced alias
            alias_sources = extract_source_columns(alias_map[col_name], alias_map, visited)
            for src in alias_sources:
                key = f"{src['table']}.{src['column']}"
                if key not in seen:
                    seen.add(key)
                    sources.append(src)
        else:
            key = f"{table}.{col.name}"
            if key not in seen:
                seen.add(key)
                sources.append({
                    "table": table,
                    "column": col.name,
                })
    return sources


def analyze_select(ast: exp.Expression, dialect: str | None, schema: dict | None, max_expr_length: int | None = None) -> dict:
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

    # Extract GROUP BY early so we can attach to aggregated columns
    group_by_cols = []
    group = qualified.find(exp.Group)
    if group:
        for expr in group.expressions:
            group_by_cols.append(expr.sql(dialect=dialect))

    # Analyze SELECT columns
    if hasattr(qualified, 'selects'):
        alias_map = build_alias_map(qualified.selects)
        for i, select_expr in enumerate(qualified.selects):
            col_info = {
                "output_position": i + 1,
                "output_name": select_expr.alias_or_name,
                "expression": truncate_expr(select_expr.sql(dialect=dialect), max_expr_length),
                "transformation": classify_transformation(select_expr),
                "data_type": infer_data_type(select_expr, schema),
                "sources": extract_source_columns(select_expr, alias_map),
            }

            # Add aggregation semantics if applicable
            agg_info = extract_aggregation_info(select_expr)
            if agg_info:
                col_info["aggregation"] = agg_info
                # Attach GROUP BY context for aggregated columns
                if group_by_cols:
                    col_info["grouped_by"] = group_by_cols

            result["columns"].append(col_info)

    # Extract JOINs
    for join in qualified.find_all(exp.Join):
        side = join.side
        kind = join.kind

        # Construct join type from side (LEFT/RIGHT) and kind (INNER/OUTER)
        if side and kind:
            join_type = f"{side} {kind}"
        elif side:
            join_type = side
        elif kind:
            join_type = kind
        else:
            join_type = "INNER"
            
        join_info = {
            "type": join_type,
            "table": join.this.name if isinstance(join.this, exp.Table) else str(join.this),
            "condition": join.args.get("on").sql(dialect=dialect) if join.args.get("on") else None,
        }
        result["joins"].append(join_info)

    # Extract WHERE filters
    where = qualified.find(exp.Where)
    if where:
        result["filters"].append(where.this.sql(dialect=dialect))

    # Add GROUP BY to result (already extracted above)
    result["group_by"] = group_by_cols

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
    max_expr_length: int | None = None,
) -> dict[str, Any]:
    """
    Perform full analysis of a SQL query.

    Returns comprehensive information about tables, columns, joins, and transformations.
    """
    try:
        ast = sqlglot.parse_one(sql, dialect=dialect)

        if isinstance(ast, exp.Select):
            return {"success": True, **analyze_select(ast, dialect, schema, max_expr_length)}

        elif isinstance(ast, exp.Create):
            # Handle CREATE TABLE ... AS SELECT
            if ast.expression and isinstance(ast.expression, exp.Select):
                result = analyze_select(ast.expression, dialect, schema, max_expr_length)
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
                select_analysis = analyze_select(ast.expression, dialect, schema, max_expr_length)
                result.update(select_analysis)
                result["query_type"] = "INSERT_SELECT"
            return {"success": True, **result}

        elif isinstance(ast, exp.Union):
            # Handle UNION queries - analyze the full union as a select-like structure
            result = analyze_select(ast, dialect, schema, max_expr_length)
            result["query_type"] = "UNION"
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


def build_cte_dependencies(ast: exp.Expression) -> dict:
    """
    Build a map of CTE dependencies.
    
    Returns: {cte_name: [list of CTEs/tables it references]}
    """
    dependencies = {}
    
    for cte in ast.find_all(exp.CTE):
        cte_name = cte.alias
        refs = []
        for table in cte.this.find_all(exp.Table):
            table_name = table.name
            if table_name:
                refs.append(table_name)
        dependencies[cte_name] = list(set(refs))
    
    return dependencies


def format_as_diagram(result: dict, dependencies: dict) -> str:
    """Format CTE dependencies as a Mermaid flowchart."""
    lines = ["```mermaid", "flowchart TD"]
    
    if not dependencies:
        lines.append("    no_ctes[No CTEs found]")
    else:
        # Collect all nodes (CTEs and tables they reference)
        cte_names = set(dependencies.keys())
        cte_names_lower = {name.lower() for name in cte_names}
        
        all_tables = set()
        for refs in dependencies.values():
            all_tables.update(refs)
        
        # Base tables: referenced tables that are not CTEs (case-insensitive check)
        base_tables = {t for t in all_tables if t.lower() not in cte_names_lower}
        
        # Add edges
        for cte_name, refs in dependencies.items():
            safe_cte = cte_name.replace("-", "_").replace(" ", "_")
            for ref in refs:
                safe_ref = ref.replace("-", "_").replace(" ", "_")
                lines.append(f"    {safe_ref} --> {safe_cte}")
        
        # Style base tables differently
        if base_tables:
            lines.append(f"    classDef baseTable fill:#2d5a2d,stroke:#4a4a4a,color:#ffffff")
            for bt in base_tables:
                safe_bt = bt.replace("-", "_").replace(" ", "_")
                lines.append(f"    class {safe_bt} baseTable")
    
    lines.append("```")
    return "\n".join(lines)


def format_as_summary(result: dict, dependencies: dict) -> str:
    """Format as a concise summary showing table-to-table dependencies."""
    lines = ["# SQL Summary\n"]
    
    # Source tables (base tables, not CTEs)
    cte_names = {cte["name"].lower() for cte in result.get("ctes", [])}
    source_tables = set()
    for t in result.get("tables", []):
        if t["name"].lower() not in cte_names:
            source_tables.add(t["name"])
    
    if source_tables:
        lines.append("## Source Tables\n")
        for t in sorted(source_tables):
            lines.append(f"- {t}")
        lines.append("")
    
    # CTE chain
    if result.get("ctes"):
        lines.append("## CTE Chain\n")
        for cte in result["ctes"]:
            refs = dependencies.get(cte["name"], [])
            if refs:
                lines.append(f"- **{cte['name']}** ← {', '.join(refs)}")
            else:
                lines.append(f"- **{cte['name']}**")
        lines.append("")
    
    # Final output
    if result.get("columns"):
        lines.append(f"## Output ({len(result['columns'])} columns)\n")
        col_names = [c["output_name"] for c in result["columns"][:10]]
        lines.append(", ".join(col_names))
        if len(result["columns"]) > 10:
            lines.append(f"... (+{len(result['columns']) - 10} more)")
        lines.append("")
    
    return "\n".join(lines)


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
        default="redshift",
        help="SQL dialect (default: redshift). Options: bigquery, snowflake, postgres, mysql, etc.",
    )
    parser.add_argument(
        "--schema", "-s",
        default=None,
        help="JSON schema string or @filepath",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "markdown", "diagram", "summary"],
        default="json",
        help="Output format (default: json). 'diagram' outputs Mermaid flowchart of CTE deps.",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--max-expr-length", "-m",
        type=int,
        default=None,
        help="Max characters for expression output (truncates with '...')",
    )

    args = parser.parse_args()

    sql = read_input(args.sql)
    schema = parse_schema(args.schema)

    result = analyze_query(sql, args.dialect, schema, args.max_expr_length)
    
    # Handle parse errors early - show error clearly regardless of format
    if not result.get("success"):
        if args.format == "json":
            output = json.dumps(result, indent=2)
        else:
            output = f"Error: {result.get('error')}\nHint: {result.get('hint', '')}"
        
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Analysis written to {args.output}")
        else:
            print(output)
        sys.exit(1)
    
    # Build CTE dependencies for diagram/summary formats
    cte_deps = {}
    if args.format in ("diagram", "summary"):
        try:
            ast = sqlglot.parse_one(sql, dialect=args.dialect)
            cte_deps = build_cte_dependencies(ast)
        except SqlglotError:
            pass

    if args.format == "markdown":
        output = format_as_markdown(result)
    elif args.format == "diagram":
        output = format_as_diagram(result, cte_deps)
    elif args.format == "summary":
        output = format_as_summary(result, cte_deps)
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
