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
uv run ~/.claude/plugins/sql-lineage/skills/sql-lineage/scripts/trace_column.py \
  "$sql" --column "$column" --format tree
```

## Response Format

Present the lineage as:

**Column `{column}` traces back to:**

```
output_column
└── intermediate_expression (if any)
    └── source_table.source_column
```

If multiple source columns contribute, show each branch.

If the trace fails, explain why and suggest:
- Providing a dialect with `--dialect`
- Providing schema with `--schema`
- Checking column name spelling
