---
name: sql-lineage-analyzer
description: |
  Deterministic SQL query analysis using sqlglot AST parsing. Use this skill when:
  - Tracing where a column/field originates from in complex queries
  - Understanding how columns are transformed through CTEs, subqueries, or joins
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

## Quick Reference

### Trace a specific column's lineage
```bash
uv run ~/.claude/plugins/sql-lineage/skills/sql-lineage/scripts/trace_column.py \
  "SELECT user_id, total FROM (SELECT id as user_id, amount as total FROM orders) t" \
  --column user_id
```

### Analyze all columns in a query
```bash
uv run ~/.claude/plugins/sql-lineage/skills/sql-lineage/scripts/analyze_query.py \
  "SELECT a.id, b.name FROM users a JOIN profiles b ON a.id = b.user_id"
```

### With schema for disambiguation
```bash
uv run ~/.claude/plugins/sql-lineage/skills/sql-lineage/scripts/trace_column.py \
  "SELECT * FROM x JOIN y ON x.id = y.id" \
  --column name \
  --schema '{"x": {"id": "INT", "name": "VARCHAR"}, "y": {"id": "INT", "email": "VARCHAR"}}'
```

### Specify SQL dialect
```bash
uv run ~/.claude/plugins/sql-lineage/skills/sql-lineage/scripts/analyze_query.py \
  "SELECT PARSE_DATE('%Y%m%d', date_str) FROM events" \
  --dialect bigquery
```

## Supported Dialects

Use the `--dialect` flag with one of:
- `bigquery` - Google BigQuery
- `snowflake` - Snowflake
- `postgres` - PostgreSQL
- `mysql` - MySQL
- `redshift` - Amazon Redshift
- `spark` - Apache Spark SQL
- `databricks` - Databricks SQL
- `duckdb` - DuckDB
- `trino` / `presto` - Trino/Presto
- `hive` - Apache Hive
- `sqlite` - SQLite
- `tsql` - T-SQL (SQL Server)
- `oracle` - Oracle

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

## For Complex Analysis

For detailed reference on sqlglot's capabilities, see [REFERENCE.md](REFERENCE.md).

For multi-step analysis or when results need interpretation, consider using the
`sql-analyst` agent which combines these tools with reasoning.
