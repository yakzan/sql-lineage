# SQL Lineage Reference

## sqlglot Core Concepts

### Abstract Syntax Tree (AST)
Every SQL query is parsed into a tree structure where each node represents a SQL element:
- `exp.Select` - SELECT statement
- `exp.Column` - Column reference
- `exp.Table` - Table reference
- `exp.CTE` - Common Table Expression
- `exp.Subquery` - Subquery
- `exp.Join` - JOIN clause

### Scopes
A scope represents a logical block of SQL with its own column namespace:
- Root query scope
- CTE scopes
- Subquery scopes
- UNION branch scopes

### Column Qualification
The process of adding table prefixes to all column references:
- `SELECT id` → `SELECT users.id`
- Required for accurate lineage when columns are ambiguous

## Script API Details

### trace_column.py

**Arguments:**
- `sql` (positional): SQL query string or `@filepath` to read from file
- `--column, -c`: Column name to trace (required)
- `--dialect, -d`: SQL dialect (default: auto-detect)
- `--schema, -s`: JSON schema string or `@filepath`
- `--format, -f`: Output format: `json` (default), `tree`, `html`

**Output (JSON):**
```json
{
  "success": true,
  "column": "user_id",
  "nodes": [
    {
      "depth": 0,
      "name": "user_id",
      "expression": "t.user_id AS user_id",
      "type": "derived"
    },
    {
      "depth": 0,
      "name": "t.user_id",
      "expression": "orders.id AS user_id",
      "type": "derived"
    },
    {
      "depth": 0,
      "name": "orders.id",
      "expression": "orders AS orders",
      "type": "table",
      "table": "orders",
      "column": "orders.id"
    }
  ],
  "edges": [
    {"from": 1, "to": 0},
    {"from": 2, "to": 1}
  ],
  "source_tables": ["orders"]
}
```

The output is a directed graph where `edges` connect nodes (child → parent).

### analyze_query.py

**Arguments:**
- `sql` (positional): SQL query string or `@filepath`
- `--dialect, -d`: SQL dialect
- `--schema, -s`: JSON schema
- `--output, -o`: Output file path
- `--format, -f`: Output format: `json` (default), `markdown`

**Output (JSON):**
```json
{
  "success": true,
  "query_type": "SELECT",
  "dialect": null,
  "tables": [
    {"name": "orders", "alias": "o", "schema": null},
    {"name": "users", "alias": "u", "schema": null}
  ],
  "ctes": [],
  "columns": [
    {
      "output_position": 1,
      "output_name": "user_name",
      "expression": "u.name AS user_name",
      "transformation": "renamed",
      "sources": [{"table": "u", "column": "name"}]
    }
  ],
  "joins": [
    {
      "type": "INNER",
      "table": "users",
      "condition": "o.user_id = u.id"
    }
  ],
  "filters": [],
  "group_by": [],
  "order_by": [],
  "aggregations": [],
  "window_functions": []
}
```

## Advanced Patterns

### Tracing Through CTEs
```sql
WITH
  step1 AS (SELECT id, amount FROM raw_orders),
  step2 AS (SELECT id, amount * 1.1 AS adjusted FROM step1)
SELECT id, adjusted FROM step2
```
The lineage for `adjusted` traces: step2.adjusted → step1.amount → raw_orders.amount

### Handling SELECT *
Without schema, SELECT * columns are marked as "unknown_source".
With schema, each column is fully traced.

### UNION Queries
Each branch is analyzed separately, with columns matched by position.

## Error Handling

Common errors and solutions:
- `ParseError`: Invalid SQL syntax - check dialect setting
- `Cannot find column 'x'`: Column not in SELECT list - check column name
- `Ambiguous column`: Provide schema or use qualified column names

## Performance Notes

- AST parsing is fast (~10ms for typical queries)
- Lineage computation scales with query complexity
- Large schemas can be provided via file to avoid shell escaping
