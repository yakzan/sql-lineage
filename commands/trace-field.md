---
name: trace-field
description: Trace the origin of a column/field in a SQL query using sqlglot AST analysis
arguments:
  - name: column
    description: The column name to trace
    required: true
  - name: sql
    description: The SQL query (or @filepath)
    required: true
---

# /trace-field

Trace a column's lineage through a SQL query.

## Instructions

When the user runs `/trace-field`, you should:

1. Parse the provided SQL query
2. Use the trace_column.py script to analyze the column's lineage
3. Present the results in a clear, hierarchical format

## Execution

Run the following command with the user's input:

```bash
uv run skills/sql-lineage/scripts/trace_column.py \
  "$sql" --column "$column" --format tree
```

Note: Default dialect is Redshift. Override with `--dialect <name>` if needed.

## Response Format

The script will return one of three results:

### 1. Column in final output
Full lineage tree from output to source tables:
```
Column: {column}

└── output_column (derived)
    └── intermediate_expression (derived)
        └── source_table.source_column (source table)
```

### 2. Column in CTE(s)
When the column is defined in a CTE but not in final output:
```
Column: {column}

Note: Column not in final SELECT, found in CTE(s):

CTE: cte_name
  Expression: the_column_expression
  Sources: table.col1, table.col2
```

### 3. Column not found
Error with helpful context:
```
Column: {column}

Error: Column 'column' not found in query
Available columns: col1, col2, col3
Available CTEs: cte1, cte2
Hint: Check spelling...
```

Present the lineage information clearly. If the column is in a CTE, explain that it's an intermediate calculation used by other parts of the query.
