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
  "column": "user_id",
  "qualified_name": "t.user_id",
  "lineage": [
    {
      "depth": 0,
      "type": "derived",
      "expression": "id AS user_id",
      "source_scope": "subquery"
    },
    {
      "depth": 1,
      "type": "table",
      "table": "orders",
      "column": "id"
    }
  ]
}
```

### analyze_query.py

**Arguments:**
- `sql` (positional): SQL query string or `@filepath`
- `--dialect, -d`: SQL dialect
- `--schema, -s`: JSON schema
- `--output, -o`: Output file path
- `--include-ctes`: Include CTE analysis (default: true)
- `--format, -f`: Output format: `json`, `markdown`, `html`

**Output (JSON):**
```json
{
  "query_type": "SELECT",
  "dialect": "postgres",
  "tables": ["orders", "users"],
  "ctes": ["active_users"],
  "columns": [
    {
      "output_name": "user_name",
      "output_position": 1,
      "transformation": "renamed",
      "sources": [
        {"table": "users", "column": "name"}
      ],
      "expression": "users.name AS user_name"
    }
  ],
  "joins": [
    {
      "type": "INNER",
      "left": "orders",
      "right": "users",
      "condition": "orders.user_id = users.id"
    }
  ],
  "filters": ["orders.status = 'complete'"],
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
