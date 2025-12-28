---
name: sql-lineage
description: Analyze a SQL query to extract all columns, tables, and transformations
arguments:
  - name: sql
    description: The SQL query to analyze (or @filepath)
    required: true
  - name: dialect
    description: SQL dialect (bigquery, snowflake, postgres, etc.)
    required: false
---

# /sql-lineage

Perform comprehensive analysis of a SQL query's structure and data flow.

## Instructions

When the user runs `/sql-lineage`, analyze the query and provide:

1. **Query Type** - SELECT, INSERT, CREATE TABLE AS SELECT, etc.
2. **Tables** - All tables referenced with their aliases
3. **CTEs** - Common Table Expressions and their output columns
4. **Output Columns** - Each SELECT column with:
   - Position and name
   - Transformation type (passthrough, renamed, derived, aggregated)
   - Source columns
5. **Joins** - Join types and conditions
6. **Filters** - WHERE clause conditions

## Execution

```bash
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/analyze_query.py \
  "$sql" --format markdown ${dialect:+--dialect "$dialect"}
```

## Follow-up Suggestions

After presenting the analysis, suggest:
- `/trace-field <column>` for deeper analysis of specific columns
- Providing schema if SELECT * was used
- Dialect specification if parsing seemed off
