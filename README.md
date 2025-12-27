# SQL Lineage

Deterministic SQL analysis using sqlglot AST parsing. Trace column lineage, understand query transformations, and debug field origins with certainty.

## Why AST-Based Analysis?

Unlike pattern matching or LLM-based approaches, AST (Abstract Syntax Tree) parsing provides:
- **Deterministic results** - same query always produces same analysis
- **Complete accuracy** - no guessing about column origins
- **Complex query support** - handles CTEs, subqueries, JOINs, UNIONs, window functions

## Quick Start

Requires [uv](https://docs.astral.sh/uv/) (Python package manager).

```bash
# Trace where a column comes from
uv run skills/sql-lineage/scripts/trace_column.py \
  "SELECT user_id FROM (SELECT id AS user_id FROM users) t" \
  --column user_id

# Analyze full query structure
uv run skills/sql-lineage/scripts/analyze_query.py \
  "SELECT a.id, b.name FROM users a JOIN profiles b ON a.id = b.user_id" \
  --format markdown
```

## Scripts

### trace_column.py
Trace a column's lineage back to source tables.

```bash
uv run skills/sql-lineage/scripts/trace_column.py "SQL" --column COLUMN [OPTIONS]

Options:
  -c, --column    Column name to trace (required)
  -d, --dialect   SQL dialect (bigquery, snowflake, postgres, mysql, etc.)
  -s, --schema    JSON schema for disambiguation
  -f, --format    Output: json (default), tree, html
```

### analyze_query.py
Extract all columns, tables, joins, and transformations.

```bash
uv run skills/sql-lineage/scripts/analyze_query.py "SQL" [OPTIONS]

Options:
  -d, --dialect   SQL dialect
  -s, --schema    JSON schema
  -f, --format    Output: json (default), markdown
  -o, --output    Write to file instead of stdout
```

### extract_tables.py
List all tables referenced in a query.

```bash
uv run skills/sql-lineage/scripts/extract_tables.py "SQL" [OPTIONS]

Options:
  -d, --dialect   SQL dialect
  --names-only    Output only table names (one per line)
```

### qualify_columns.py
Add table prefixes to all column references.

```bash
uv run skills/sql-lineage/scripts/qualify_columns.py "SQL" [OPTIONS]

Options:
  -d, --dialect   SQL dialect
  -s, --schema    JSON schema (recommended)
  --sql-only      Output only the qualified SQL
```

## Supported SQL Dialects

| Dialect | Flag |
|---------|------|
| Google BigQuery | `--dialect bigquery` |
| Snowflake | `--dialect snowflake` |
| PostgreSQL | `--dialect postgres` |
| MySQL | `--dialect mysql` |
| Amazon Redshift | `--dialect redshift` |
| Apache Spark SQL | `--dialect spark` |
| Databricks | `--dialect databricks` |
| DuckDB | `--dialect duckdb` |
| Trino/Presto | `--dialect trino` |
| Apache Hive | `--dialect hive` |
| SQLite | `--dialect sqlite` |
| SQL Server (T-SQL) | `--dialect tsql` |
| Oracle | `--dialect oracle` |

## Schema Format

For queries with `SELECT *` or ambiguous columns, provide schema as JSON:

```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "SELECT * FROM users JOIN orders ON users.id = orders.user_id" \
  --column email \
  --schema '{"users": {"id": "INT", "email": "VARCHAR"}, "orders": {"id": "INT", "user_id": "INT"}}'
```

Or from a file:
```bash
--schema @schema.json
```

## Claude Code Plugin Installation

This project is a Claude Code plugin. To install:

```bash
# Option 1: Symlink for development
ln -sfn $(pwd) ~/.claude/plugins/sql-lineage

# Option 2: Install from GitHub (when published)
# /plugin install github.com/your-username/sql-lineage-plugin
```

Once installed, use slash commands:
- `/trace-field` - Quick column lineage tracing
- `/sql-lineage` - Full query analysis

## Examples

### Trace through CTEs
```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "WITH step1 AS (SELECT id, amount FROM orders),
        step2 AS (SELECT id, amount * 1.1 AS adjusted FROM step1)
   SELECT adjusted FROM step2" \
  --column adjusted
```

### BigQuery-specific syntax
```bash
uv run skills/sql-lineage/scripts/analyze_query.py \
  "SELECT PARSE_DATE('%Y%m%d', date_str) AS parsed FROM events" \
  --dialect bigquery
```

### HTML visualization
```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "SELECT ..." --column col --format html > lineage.html
```

## License

MIT
