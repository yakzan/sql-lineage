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
      "sources": [{"table": "u", "column": "name"}],
      "data_type": "UNKNOWN"
    },
    {
      "output_position": 2,
      "output_name": "total",
      "expression": "SUM(o.amount) AS total",
      "transformation": "aggregated",
      "sources": [{"table": "o", "column": "amount"}],
      "data_type": "NUMERIC",
      "aggregation": {
        "function": "SUM",
        "input_columns": ["o.amount"]
      },
      "grouped_by": ["u.name"]
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

### impact_analysis.py

**Arguments:**
- `sql` (positional): SQL query string or `@filepath`
- `--source-column, -c`: Source column to analyze (required). Can be `table.column` or just `column`
- `--dialect, -d`: SQL dialect (default: redshift)
- `--format, -f`: Output format: `json` (default), `tree`, `graph`
- `--max-expr-length`: Maximum length for expression strings (default: unlimited)
- `--max-sources`: Maximum number of available source columns to return (default: unlimited)
- `--summary-only`: Omit expression fields for lightweight output (ideal for agents)
- `--include-line-numbers`: Include line numbers where CTEs and SELECT are defined
- `--include-graph`: Include a node/edge graph in JSON (or use `-f graph` to emit only the graph)
- `--diff-old / --diff-new`: Compare two SQL versions for the same source column (reverse-lineage diff)
- Diff mode supports `json`/`graph` output only and requires full expressions (no `--summary-only` or `--max-expr-length`).
- Columns are qualified with sqlglot’s `qualify`, so both table aliases and base table names are accepted (e.g., `o.status` or `orders.status`).
- UNION branches are analyzed separately; source columns remain branch-specific (e.g., `orders.status` vs `archived_orders.status`).
- Inline subqueries are traversed like anonymous CTEs, so impact flows through them as well:
  ```sql
  SELECT t.doubled FROM (SELECT amount * 2 AS doubled FROM orders) t
  -- Changing orders.amount correctly impacts output.doubled
  ```

**Output (JSON):**
```json
{
  "success": true,
  "source_column": "orders.status",
  "impact_summary": {
    "output_columns_affected": 2,
    "cte_columns_affected": 5,
    "total_affected": 7
  },
  "impacted_output_columns": [
    {
      "column": "status_flag",
      "position": 3,
      "expression": "CASE WHEN status > 90 THEN 'cancelled' END"
    }
  ],
  "impacted_cte_columns": [
    {
      "cte": "order_stats",
      "column": "cancel_count",
      "expression": "SUM(CASE WHEN status = 91 THEN 1 END)"
    }
  ],
  "available_source_columns": ["orders.id", "orders.status", "orders.amount"]
}
```

**Output with `--include-line-numbers`:**
```json
{
  "success": true,
  "line_numbers": {
    "cte:order_stats": 2,
    "cte:metrics": 8,
    "final_select": 15
  },
  "impacted_cte_columns": [
    {
      "cte": "order_stats",
      "column": "cancel_count",
      "line_hint": 2
    }
  ]
}
```

**Output with `--summary-only`:**
Omits the `expression` field from all columns, reducing output size significantly for large queries.

**Graph output (`-f graph` or `--include-graph`):**
Returns a machine-parseable structure:
```json
{
  "nodes": [
    {"id": "orders.status", "kind": "source", "label": "orders.status"},
    {"id": "output.status_flag", "kind": "output", "column": "status_flag", "label": "output.status_flag"}
  ],
  "edges": [{"source": "orders.status", "target": "output.status_flag"}]
}
```
Node `kind` values: `source`, `output`, `cte`, `subquery`.

**Diff impact (`--diff-old/--diff-new`):**
Requires full expressions (no summary/truncation) and outputs `json` or `graph`.
```json
{
  "success": true,
  "source_column": "orders.status",
  "diff_summary": {
    "outputs_added": 1,
    "outputs_removed": 0,
    "outputs_changed": 0,
    "ctes_added": 0,
    "ctes_removed": 0,
    "ctes_changed": 1
  },
  "outputs": {
    "added": [{"column": "new_flag", "position": 4, "expression": "..."}],
    "removed": [],
    "changed": []
  },
  "ctes": {
    "added": [],
    "removed": [],
    "changed": [
      {
        "name": "order_stats.cancel_rate",
        "old": {"cte": "order_stats", "column": "cancel_rate", "expression": "canceled / total"},
        "new": {"cte": "order_stats", "column": "cancel_rate", "expression": "canceled / NULLIF(total, 0)"}
      }
    ]
  },
  "graphs": null
}
```
When `--include-graph` is used with diff, `graphs` contains `{"old": {...}, "new": {...}}`.

**Agent Workflow Pattern:**
For large queries with many CTEs, use the two-phase approach:
```bash
# Phase 1: Get lightweight impact map
uv run impact_analysis.py @query.sql -c status --summary-only --include-line-numbers

# Phase 2: Read specific CTE lines if needed (using the line_hint)
# The agent can use Read tool with offset/limit to fetch just those lines
```

**Data Type Inference:**

| Expression Type | Inferred Type |
|----------------|---------------|
| COUNT(*), COUNT(col) | BIGINT |
| SUM(col) | NUMERIC |
| AVG(col) | DOUBLE |
| MIN/MAX(col) | INHERITED |
| CAST(x AS TYPE) | TYPE |
| CASE with strings | VARCHAR |
| CASE with numbers | NUMERIC |
| Arithmetic (+, -, *, /) | NUMERIC |
| EXTRACT(... FROM date) | INTEGER |

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
