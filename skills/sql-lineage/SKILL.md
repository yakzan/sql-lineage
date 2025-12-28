---
name: sql-lineage-analyzer
description: |
  Deterministic SQL query analysis using sqlglot AST parsing. Use this skill when:
  - Tracing where a column/field originates from in complex queries
  - Understanding how columns are transformed through CTEs, subqueries, or joins
  - Finding column definitions in CTEs (even if not in final output)
  - Debugging SQL queries to find field sources
  - Analyzing query structure and column dependencies
  - Extracting table and column metadata from SQL
  Never guess about column origins - always use these tools for certainty.
allowed-tools: Read, Bash, Write, Grep, Glob
---

# SQL Lineage Analyzer

This skill provides deterministic SQL analysis using sqlglot's Abstract Syntax Tree (AST)
parsing. Unlike probabilistic approaches, AST-based analysis gives you certainty about
column origins and transformations.

## Key Features

- **Default dialect: Redshift** - No need to specify dialect for Redshift queries
- **CTE-aware column tracing** - Finds columns in CTEs even if not in final SELECT
- **Never fails silently** - Always returns useful context about what was found

## Quick Reference

### Trace a specific column's lineage
```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "SELECT user_id, total FROM (SELECT id as user_id, amount as total FROM orders) t" \
  --column user_id
```

### Trace a column in a CTE (new!)
```bash
# Even if total_amount is only in a CTE (not final output), this works:
uv run skills/sql-lineage/scripts/trace_column.py \
  @query.sql --column total_amount --format tree
```

### Analyze all columns in a query
```bash
uv run skills/sql-lineage/scripts/analyze_query.py \
  "SELECT a.id, b.name FROM users a JOIN profiles b ON a.id = b.user_id"
```

### With schema for disambiguation
```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "SELECT * FROM x JOIN y ON x.id = y.id" \
  --column name \
  --schema '{"x": {"id": "INT", "name": "VARCHAR"}, "y": {"id": "INT", "email": "VARCHAR"}}'
```

### Specify SQL dialect (if not Redshift)
```bash
uv run skills/sql-lineage/scripts/analyze_query.py \
  "SELECT PARSE_DATE('%Y%m%d', date_str) FROM events" \
  --dialect bigquery
```

## Supported Dialects

Use the `--dialect` flag with one of:
- `redshift` - Amazon Redshift **(default)**
- `bigquery` - Google BigQuery
- `snowflake` - Snowflake
- `postgres` - PostgreSQL
- `mysql` - MySQL
- `spark` - Apache Spark SQL
- `databricks` - Databricks SQL
- `duckdb` - DuckDB
- `trino` / `presto` - Trino/Presto
- `hive` - Apache Hive
- `sqlite` - SQLite
- `tsql` - T-SQL (SQL Server)
- `oracle` - Oracle

## trace_column.py Output Types

The trace script returns one of three result types:

### 1. Column in final output
Full lineage tree with nodes and edges for visualization.

### 2. Column in CTE(s)
```json
{
  "success": true,
  "column": "total_amount",
  "in_final_output": false,
  "found_in": [
    {
      "location": "cte",
      "cte_name": "monthly_totals",
      "expression": "sum(...) / sum(expected_value)",
      "sources": ["daily_totals.actual_value", "daily_totals.expected_value"]
    }
  ],
  "available_ctes": ["daily_totals", "monthly_totals", "weekly_totals"]
}
```

**Tracing through multiple CTEs:** If a source (e.g., `daily_totals.actual_value`) references another CTE in `available_ctes`, call `trace_column.py` again with that column to trace further back to source tables.

### 3. Column not found
```json
{
  "success": false,
  "column": "unknown_col",
  "error": "Column 'unknown_col' not found in query",
  "available_in_output": ["id", "status"],
  "available_ctes": ["monthly_totals", "weekly_totals", "base"],
  "hint": "Check spelling. Use analyze_query.py to see all CTEs and their columns."
}
```

## When to Use Schema Information

Schema is required when:
1. Query uses `SELECT *` - need to know which columns exist
2. Columns are referenced without table prefix in JOINs
3. Column could come from multiple tables (ambiguous reference)

Schema format (JSON):
```json
{
  "table_name": {
    "column1": "TYPE",
    "column2": "TYPE"
  }
}
```

## Understanding Output

### Lineage Node Types
- **table**: Column comes directly from a source table
- **derived**: Column is computed/transformed from other columns
- **subquery**: Column comes from a subquery or CTE

### Transformation Types
- **passthrough**: Column passes through unchanged (e.g., `SELECT id FROM t`)
- **renamed**: Column is aliased (e.g., `SELECT id AS user_id`)
- **derived**: Column is computed (e.g., `SELECT a + b AS total`)
- **aggregated**: Column uses aggregation (e.g., `SELECT SUM(amount)`)

## Tracing Through Multiple CTE Levels

When a column is defined in a CTE that references another CTE, you can trace the full lineage by making multiple calls:

```bash
# Step 1: Find where `final_metric` is defined
uv run skills/sql-lineage/scripts/trace_column.py @query.sql --column final_metric
# Returns: found_in cte3, sources: ["cte2.intermediate"], available_ctes: ["cte1", "cte2", "cte3"]

# Step 2: "cte2" is in available_ctes, so trace the source column
uv run skills/sql-lineage/scripts/trace_column.py @query.sql --column intermediate
# Returns: found_in cte2, sources: ["cte1.base_value"], available_ctes: [...]

# Step 3: Continue until sources point to actual tables (not CTEs)
uv run skills/sql-lineage/scripts/trace_column.py @query.sql --column base_value
# Returns: found_in cte1, sources: ["orders.amount"] - done! orders is a table
```

This agent-driven approach keeps the tool simple while allowing full traceability.

## For Complex Analysis

For detailed reference on sqlglot's capabilities, see [REFERENCE.md](REFERENCE.md).

For multi-step analysis or when results need interpretation, consider using the
`sql-analyst` agent which combines these tools with reasoning.
