#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
"""
Analyze the impact of changing a source column.

Given a source column (e.g., orders.status), find all output columns
and CTEs that depend on it. This is "reverse lineage" or "impact analysis".

Usage:
    uv run impact_analysis.py @query.sql --source-column orders.status
    uv run impact_analysis.py @query.sql --source-column o.amount --format tree
"""

import argparse
import json
import re
import sys
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify import qualify


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


def truncate_expr(expr: str | None, max_length: int | None) -> str | None:
    """Truncate expression to max_length if specified."""
    if expr is None or max_length is None or max_length <= 0:
        return expr
    if len(expr) <= max_length:
        return expr
    return expr[:max_length] + "..."


def find_line_numbers(sql: str, cte_names: set[str]) -> dict[str, int]:
    """
    Find line numbers where CTEs and final SELECT are defined.

    Returns a dict mapping:
    - "cte:<cte_name>" -> line number where CTE starts
    - "final_select" -> line number where final SELECT starts
    """
    lines = sql.split('\n')
    line_info = {}

    # Track if we're inside a CTE block
    in_with_clause = False

    for i, line in enumerate(lines, 1):
        line_upper = line.upper().strip()

        # Detect start of WITH clause
        if line_upper.startswith('WITH ') or line_upper == 'WITH':
            in_with_clause = True

        # Look for CTE definitions: "cte_name AS (" or "cte_name AS"
        for cte_name in cte_names:
            # Pattern: cte_name followed by AS (case-insensitive)
            pattern = rf'\b{re.escape(cte_name)}\s+AS\s*[\(\n]'
            if re.search(pattern, line, re.IGNORECASE):
                line_info[f"cte:{cte_name}"] = i
                break

        # Detect final SELECT (SELECT not inside WITH clause definition)
        # This is the SELECT that comes after all CTEs
        if "final_select" not in line_info:
            # Look for SELECT that's not part of a CTE definition
            # A final SELECT typically starts at column 0 or follows a closing paren
            if line_upper.startswith('SELECT ') or line_upper == 'SELECT':
                # Check if this is after the CTE definitions
                # (heuristic: if we've found CTEs and this SELECT is at the start of line)
                if not in_with_clause or (cte_names and len(line_info) >= len(cte_names)):
                    line_info["final_select"] = i

        # Detect end of WITH clause (when we hit the final SELECT)
        if in_with_clause and line_upper.startswith('SELECT '):
            # Check if all CTEs have been found
            if len([k for k in line_info if k.startswith("cte:")]) >= len(cte_names):
                in_with_clause = False
                if "final_select" not in line_info:
                    line_info["final_select"] = i

    return line_info


def build_alias_map(selects: list[exp.Expression]) -> dict[str, exp.Expression]:
    """Build a map of column aliases to their expressions."""
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
    alias_table_map: dict[str, str] | None = None,
    _visited: set[str] | None = None,
) -> set[str]:
    """Extract all source columns from an expression, resolving self-references.

    alias_table_map lets us expand table aliases to their base table names so
    reverse-lineage can match both `o.status` and `orders.status`.
    """
    sources = set()
    alias_map = alias_map or {}
    alias_table_map = alias_table_map or {}
    visited = _visited if _visited is not None else set()

    for col in expr.find_all(exp.Column):
        table = (col.table or "unknown").lower()
        col_name = col.name.lower()

        if table == "unknown" and col_name in alias_map and col_name not in visited:
            visited.add(col_name)
            alias_sources = extract_source_columns(
                alias_map[col_name], alias_map, alias_table_map, visited
            )
            sources.update(alias_sources)
        else:
            # Always keep the alias-qualified reference
            sources.add(f"{table}.{col_name}")

            # Also add base-table-qualified reference when available
            base_table = alias_table_map.get(table)
            if base_table:
                sources.add(f"{base_table}.{col_name}")

    return sources


def build_dependency_graph(
    ast: exp.Expression,
    max_expr_length: int | None = None,
    dialect: str | None = None,
) -> dict[str, dict]:
    """
    Build a dependency graph for all columns in the query.

    Returns a dict where:
    - Keys are column identifiers (e.g., "cte_name.column_name" or "output.column_name")
    - Values are dicts with:
        - "sources": set of source column identifiers this column depends on
        - "location": where this column is defined ("cte", "output", "subquery")
        - "expression": the SQL expression
    """
    graph = {}
    processed_subqueries: set[int] = set()
    subquery_counter = 0

    def next_subquery_name() -> str:
        nonlocal subquery_counter
        subquery_counter += 1
        return f"subq{subquery_counter}"

    def collect_table_aliases(relation: exp.Expression, subquery_aliases: dict[str, str]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for table in relation.find_all(exp.Table):
            alias = (table.alias or table.name or "").lower()
            base = (table.name or "").lower()
            if alias:
                mapping[alias] = base
        # Map subquery aliases to their generated names so impact can traverse them
        mapping.update(subquery_aliases)
        return mapping

    def process_relation(relation: exp.Expression, location: str, name: str):
        # Walk UNION branches explicitly so both sides contribute dependencies
        if isinstance(relation, exp.Union):
            process_relation(relation.left, location, f"{name}_left")
            process_relation(relation.right, location, f"{name}_right")
            return

        if not hasattr(relation, "selects"):
            return

        # Discover inline subqueries in this relation (FROM / JOIN, etc.)
        subquery_aliases: dict[str, str] = {}
        for sub in relation.find_all(exp.Subquery):
            sub_id = id(sub)
            if sub_id in processed_subqueries:
                continue
            processed_subqueries.add(sub_id)

            sub_alias = (sub.alias or "").lower()
            sub_name = sub_alias or next_subquery_name()
            process_relation(sub.this, "subquery", sub_name)
            if sub_alias:
                subquery_aliases[sub_alias] = sub_name

        alias_map = build_alias_map(relation.selects)
        alias_table_map = collect_table_aliases(relation, subquery_aliases)

        for i, sel in enumerate(relation.selects):
            col_name = sel.alias_or_name.lower()
            col_id = f"{name}.{col_name}"
            sources = extract_source_columns(sel, alias_map, alias_table_map)
            graph[col_id] = {
                "sources": sources,
                "location": location,
                "cte_name": name if location in {"cte", "subquery"} else None,
                "output_position": i + 1 if location == "output" else None,
                "column_name": col_name,
                "expression": truncate_expr(sel.sql(dialect=dialect), max_expr_length),
            }

    # Process CTEs
    for cte in ast.find_all(exp.CTE):
        cte_name = cte.alias.lower()
        process_relation(cte.this, "cte", cte_name)

    # Process final output (top-level select or union)
    process_relation(ast, "output", "output")

    return graph


def build_reverse_index(graph: dict[str, dict]) -> dict[str, set[str]]:
    """
    Build a reverse index: source_column -> set of columns that depend on it.
    """
    reverse_index = {}

    for col_id, info in graph.items():
        for source in info["sources"]:
            if source not in reverse_index:
                reverse_index[source] = set()
            reverse_index[source].add(col_id)

    return reverse_index


def find_impacted_columns(
    source_column: str,
    graph: dict[str, dict],
    reverse_index: dict[str, set[str]],
    derived_names: set[str],
) -> dict[str, Any]:
    """
    Find all columns impacted by a change to source_column.

    Uses BFS to find transitive dependencies.
    """
    source_lower = source_column.lower()

    # Normalize source column (handle table.column or just column)
    if "." not in source_lower:
        # If no table specified, search for any column with this name
        matching_sources = [s for s in reverse_index.keys() if s.endswith(f".{source_lower}")]
        if not matching_sources:
            return {
                "success": False,
                "error": f"Source column '{source_column}' not found in query",
                "available_sources": sorted(list(reverse_index.keys()))[:20],
            }
        # Use all matching sources
        sources_to_check = set(matching_sources)
    else:
        sources_to_check = {source_lower}

    # BFS to find all impacted columns
    impacted = set()
    visited = set()
    queue = list(sources_to_check)

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        # Find direct dependents
        dependents = reverse_index.get(current, set())
        for dep in dependents:
            impacted.add(dep)
            # Also check if this dependent is used by other columns
            # (CTE columns can be referenced by other CTEs or output)
            parts = dep.split(".")
            if len(parts) == 2:
                relation, col = parts
                if relation in derived_names:
                    # This is a derived relation (CTE or subquery); keep traversing
                    cte_col_ref = f"{relation}.{col}"
                    unknown_col_ref = f"unknown.{col}"
                    if cte_col_ref not in visited:
                        queue.append(cte_col_ref)
                    if unknown_col_ref not in visited:
                        queue.append(unknown_col_ref)

    # Categorize impacted columns
    impacted_output = []
    impacted_ctes = []

    for col_id in impacted:
        info = graph.get(col_id, {})
        if info.get("location") == "output":
            impacted_output.append({
                "column": info.get("column_name"),
                "position": info.get("output_position"),
                "expression": info.get("expression"),
            })
        elif info.get("location") == "cte":
            impacted_ctes.append({
                "cte": info.get("cte_name"),
                "column": info.get("column_name"),
                "expression": info.get("expression"),
            })

    return {
        "success": True,
        "source_column": source_column,
        "impact_summary": {
            "output_columns_affected": len(impacted_output),
            "cte_columns_affected": len(impacted_ctes),
            "total_affected": len(impacted),
        },
        "impacted_output_columns": sorted(impacted_output, key=lambda x: x.get("position", 0)),
        "impacted_cte_columns": impacted_ctes,
    }


def analyze_impact(
    sql: str,
    source_column: str,
    dialect: str | None = None,
    max_expr_length: int | None = None,
    max_sources: int | None = None,
    summary_only: bool = False,
    include_line_numbers: bool = False,
    include_graph: bool = False,
) -> dict[str, Any]:
    """
    Analyze the impact of changing a source column.

    Args:
        sql: The SQL query to analyze
        source_column: The source column to check (e.g., "orders.status" or just "status")
        dialect: SQL dialect
        max_expr_length: Maximum length for expression strings (None = unlimited)
        max_sources: Maximum number of available source columns to return (None = unlimited)
        summary_only: If True, omit expression fields for lightweight output
        include_line_numbers: If True, include line numbers where columns are defined

    Returns:
        Dict with impact analysis results
    """
    dialect = dialect or "redshift"

    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except SqlglotError as e:
        return {
            "success": False,
            "error": f"Parse error: {e}",
            "hint": "Check SQL syntax or try a different dialect",
        }

    # Qualify columns so dependency graph keeps base-table names (when available)
    try:
        ast = qualify(parsed, dialect=dialect, validate_qualify_columns=False)
    except SqlglotError:
        ast = parsed

    # Build dependency graph
    graph = build_dependency_graph(ast, max_expr_length, dialect)

    # Build reverse index
    reverse_index = build_reverse_index(graph)

    # Get derived relation names (CTEs and inline subqueries) for transitive dependency tracking
    derived_names = {key.split(".")[0] for key in graph.keys() if key.split(".")[0] not in {"output", "unknown"}}

    # Find impacted columns
    result = find_impacted_columns(source_column, graph, reverse_index, derived_names)

    # Add available source columns for reference
    if result.get("success"):
        all_sources = sorted(list(reverse_index.keys()))
        if max_sources and max_sources > 0:
            result["available_source_columns"] = all_sources[:max_sources]
        else:
            result["available_source_columns"] = all_sources

        # Add line numbers if requested
        if include_line_numbers:
            cte_names = {cte.alias.lower() for cte in ast.find_all(exp.CTE)}
            line_info = find_line_numbers(sql, cte_names)
            result["line_numbers"] = line_info

            # Add line numbers to impacted output columns
            final_select_line = line_info.get("final_select")
            for col in result.get("impacted_output_columns", []):
                if final_select_line:
                    col["line_hint"] = final_select_line

            # Add line numbers to impacted CTE columns
            for col in result.get("impacted_cte_columns", []):
                cte_name = col.get("cte")
                if cte_name:
                    cte_line = line_info.get(f"cte:{cte_name}")
                    if cte_line:
                        col["line_hint"] = cte_line

        # Remove expressions if summary_only
        if summary_only:
            for col in result.get("impacted_output_columns", []):
                col.pop("expression", None)
            for col in result.get("impacted_cte_columns", []):
                col.pop("expression", None)
        if include_graph:
            result["graph"] = export_graph(graph)

    return result


def export_graph(graph: dict[str, dict]) -> dict[str, list[dict[str, str]]]:
    """Convert internal graph to a machine-parseable node/edge structure."""
    nodes: dict[str, dict[str, str]] = {}
    edges: list[dict[str, str]] = []

    def add_node(node_id: str, kind: str, extra: dict[str, str] | None = None):
        if node_id not in nodes:
            nodes[node_id] = {"id": node_id, "kind": kind}
            if extra:
                nodes[node_id].update(extra)

    for col_id, info in graph.items():
        location = info.get("location", "unknown")
        add_node(
            col_id,
            kind=location,
            extra={
                "column": info.get("column_name", ""),
                "label": f"{col_id}",
            },
        )
        for src in info.get("sources", []):
            add_node(src, kind="source", extra={"label": src})
            edges.append({"source": src, "target": col_id})

    return {"nodes": list(nodes.values()), "edges": edges}


def diff_impact(
    old_sql: str,
    new_sql: str,
    source_column: str,
    dialect: str | None = None,
    max_expr_length: int | None = None,
    summary_only: bool = False,
    include_graph: bool = False,
) -> dict[str, Any]:
    """Compare impact across two SQL versions."""
    if summary_only or max_expr_length not in {None, 0}:
        return {"success": False, "error": "Diff mode requires full expressions; remove summary/truncation flags"}
    old_result = analyze_impact(
        old_sql,
        source_column,
        dialect=dialect,
        max_expr_length=max_expr_length,
        summary_only=summary_only,
        include_graph=include_graph,
    )
    if not old_result.get("success"):
        return {"success": False, "error": f"Old SQL failed: {old_result.get('error')}"}

    new_result = analyze_impact(
        new_sql,
        source_column,
        dialect=dialect,
        max_expr_length=max_expr_length,
        summary_only=summary_only,
        include_graph=include_graph,
    )
    if not new_result.get("success"):
        return {"success": False, "error": f"New SQL failed: {new_result.get('error')}"}

    def index_output(cols: Iterable[dict]) -> dict[str, dict]:
        return {f"output.{c['column']}": c for c in cols}

    def index_cte(cols: Iterable[dict]) -> dict[str, dict]:
        return {f"{c['cte']}.{c['column']}": c for c in cols}

    old_out = index_output(old_result.get("impacted_output_columns", []))
    new_out = index_output(new_result.get("impacted_output_columns", []))
    old_cte = index_cte(old_result.get("impacted_cte_columns", []))
    new_cte = index_cte(new_result.get("impacted_cte_columns", []))

    def diff_maps(old_map: dict[str, dict], new_map: dict[str, dict]):
        added = sorted(set(new_map) - set(old_map))
        removed = sorted(set(old_map) - set(new_map))
        changed = []
        for key in set(old_map).intersection(new_map):
            if old_map[key].get("expression") != new_map[key].get("expression"):
                changed.append(key)
        return added, removed, sorted(changed)

    added_out, removed_out, changed_out = diff_maps(old_out, new_out)
    added_cte, removed_cte, changed_cte = diff_maps(old_cte, new_cte)

    return {
        "success": True,
        "source_column": source_column,
        "diff_summary": {
            "outputs_added": len(added_out),
            "outputs_removed": len(removed_out),
            "outputs_changed": len(changed_out),
            "ctes_added": len(added_cte),
            "ctes_removed": len(removed_cte),
            "ctes_changed": len(changed_cte),
        },
        "outputs": {
            "added": [new_out[k] for k in added_out],
            "removed": [old_out[k] for k in removed_out],
            "changed": [{"name": k, "old": old_out[k], "new": new_out[k]} for k in changed_out],
        },
        "ctes": {
            "added": [new_cte[k] for k in added_cte],
            "removed": [old_cte[k] for k in removed_cte],
            "changed": [{"name": k, "old": old_cte[k], "new": new_cte[k]} for k in changed_cte],
        },
        "graphs": {
            "old": old_result.get("graph"),
            "new": new_result.get("graph"),
        } if include_graph else None,
    }


def format_as_tree(result: dict) -> str:
    """Format impact analysis result as a tree."""
    if not result.get("success"):
        return f"Error: {result.get('error')}\n\nAvailable sources:\n" + \
               "\n".join(f"  - {s}" for s in result.get("available_sources", [])[:20])

    lines = [
        f"Impact Analysis for: {result['source_column']}",
        "",
        f"Summary: {result['impact_summary']['total_affected']} columns affected",
        f"  - Output columns: {result['impact_summary']['output_columns_affected']}",
        f"  - CTE columns: {result['impact_summary']['cte_columns_affected']}",
        "",
    ]

    if result["impacted_output_columns"]:
        lines.append("Impacted Output Columns:")
        for col in result["impacted_output_columns"]:
            line_hint = f" (line ~{col['line_hint']})" if col.get("line_hint") else ""
            lines.append(f"  [{col['position']}] {col['column']}{line_hint}")
            if col.get("expression"):
                expr = col["expression"][:80] + "..." if len(col.get("expression", "")) > 80 else col.get("expression", "")
                lines.append(f"      Expression: {expr}")
        lines.append("")

    if result["impacted_cte_columns"]:
        lines.append("Impacted CTE Columns:")
        # Group by CTE
        by_cte = {}
        for col in result["impacted_cte_columns"]:
            cte = col["cte"]
            if cte not in by_cte:
                by_cte[cte] = []
            by_cte[cte].append(col)

        for cte, cols in by_cte.items():
            # Get line hint from first column in this CTE
            line_hint = ""
            if cols and cols[0].get("line_hint"):
                line_hint = f" (line ~{cols[0]['line_hint']})"
            lines.append(f"  CTE: {cte}{line_hint}")
            for col in cols:
                lines.append(f"    - {col['column']}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze the impact of changing a source column (reverse lineage)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find what depends on orders.status
  uv run impact_analysis.py @query.sql --source-column orders.status

  # Find what depends on any 'amount' column
  uv run impact_analysis.py @query.sql --source-column amount

  # Tree format for readability
  uv run impact_analysis.py @query.sql --source-column o.status --format tree
        """,
    )

    parser.add_argument(
        "sql",
        nargs="?",
        help="SQL query string, or @filepath to read from file",
    )
    parser.add_argument(
        "--source-column", "-c",
        required=True,
        help="Source column to analyze impact for (e.g., 'orders.status' or just 'status')",
    )
    parser.add_argument(
        "--dialect", "-d",
        default="redshift",
        help="SQL dialect (default: redshift)",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["json", "tree", "graph"],
        default="json",
        help="Output format (default: json)",
    )
    parser.add_argument(
        "--max-expr-length",
        type=int,
        default=None,
        help="Maximum length for expression strings (default: unlimited)",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=None,
        help="Maximum number of available source columns to return (default: unlimited)",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Omit expressions for lightweight output (agent-friendly)",
    )
    parser.add_argument(
        "--include-line-numbers",
        action="store_true",
        help="Include line numbers where CTEs and columns are defined",
    )
    parser.add_argument(
        "--include-graph",
        action="store_true",
        help="Include node/edge graph data in JSON output",
    )
    parser.add_argument(
        "--diff-old",
        help="Old SQL string or @filepath for impact diff mode",
    )
    parser.add_argument(
        "--diff-new",
        help="New SQL string or @filepath for impact diff mode",
    )

    args = parser.parse_args()
    want_graph = args.include_graph or args.format == "graph"

    # Diff mode requires both old and new SQL
    if args.diff_old or args.diff_new:
        if not (args.diff_old and args.diff_new):
            sys.exit("Error: --diff-old and --diff-new must be provided together")
        if args.format == "tree":
            sys.exit("Error: diff mode supports only json or graph output")
        if args.summary_only:
            sys.exit("Error: --summary-only is not supported in diff mode (needs full expressions to compare)")
        if args.max_expr_length not in {None, 0}:
            sys.exit("Error: --max-expr-length is not supported in diff mode (needs full expressions to compare)")
        old_sql = read_input(args.diff_old)
        new_sql = read_input(args.diff_new)
        result = diff_impact(
            old_sql,
            new_sql,
            args.source_column,
            dialect=args.dialect,
            max_expr_length=args.max_expr_length,
            summary_only=args.summary_only,
            include_graph=want_graph,
        )
    else:
        if not args.sql:
            sys.exit("Error: SQL is required unless using diff mode")
        sql = read_input(args.sql)
        result = analyze_impact(
            sql,
            args.source_column,
            args.dialect,
            args.max_expr_length,
            args.max_sources,
            args.summary_only,
            args.include_line_numbers,
            want_graph,
        )

    if args.format == "tree":
        print(format_as_tree(result))
    elif args.format == "graph":
        print(json.dumps(result.get("graph") or result.get("graphs"), indent=2))
    else:
        print(json.dumps(result, indent=2))

    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
