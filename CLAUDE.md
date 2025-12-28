# CLAUDE.md - SQL Lineage Plugin

## Project Overview

Claude Code plugin providing deterministic SQL analysis via sqlglot AST parsing. No guessing about column origins - AST analysis gives certainty.

## Architecture

```
.
├── .claude-plugin/
│   └── plugin.json           # Plugin manifest
├── skills/sql-lineage/
│   ├── SKILL.md              # Claude reads this for skill instructions
│   ├── REFERENCE.md          # Detailed API docs
│   └── scripts/
│       ├── trace_column.py   # Column lineage tracing
│       ├── analyze_query.py  # Full query analysis
│       ├── extract_tables.py # Table extraction
│       └── qualify_columns.py# Column qualification
├── agents/
│   └── sql-analyst.md        # SQL analysis agent definition
└── commands/
    ├── trace-field.md        # /trace-field slash command
    └── sql-lineage.md        # /sql-lineage slash command
```

## Key Conventions

### PEP 723 Inline Dependencies
All Python scripts use inline script metadata - no requirements.txt:
```python
#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "sqlglot[rs]>=26.0.0",
# ]
# ///
```

Run scripts with `uv run script.py` - dependencies auto-install on first run.

### Script Input Patterns
- SQL as string: `"SELECT ..."`
- SQL from file: `@query.sql`
- Schema as JSON string: `--schema '{"table": {"col": "TYPE"}}'`
- Schema from file: `--schema @schema.json`

### Default Dialect
Default dialect is **Redshift**. Override with `--dialect <name>` for other databases.

### Output Formats
- `--format json` - Structured data (default)
- `--format tree` - Human-readable hierarchy (trace_column.py)
- `--format markdown` - Documentation-ready (analyze_query.py)
- `--format html` - Interactive visualization (trace_column.py)

## Testing

Run the full test suite:
```bash
uv run pytest tests/ -v
```

**Test Guidelines:**
- **NEVER mock or patch** - tests must call real functions and verify actual behavior
- Run scripts directly to verify behavior before writing tests
- Assert on actual output, not assumed/expected output

Manual test examples:
```bash
# Test 1: Basic column tracing
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/trace_column.py \
  "SELECT user_id FROM (SELECT id AS user_id FROM users) t" \
  --column user_id

# Test 2: CTE analysis
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/analyze_query.py \
  "WITH active AS (SELECT id FROM users WHERE active) SELECT id FROM active" \
  --format markdown

# Test 3: Join analysis
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/trace_column.py \
  "SELECT o.amount FROM orders o JOIN users u ON o.user_id = u.id" \
  --column amount

# Test 4: Table extraction
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/extract_tables.py \
  "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id" \
  --names-only

# Test 5: CTE column tracing (column not in final output)
uv run ${CLAUDE_PLUGIN_ROOT}/skills/sql-lineage/scripts/trace_column.py \
  "WITH totals AS (SELECT id, amount * 2 AS doubled FROM orders) SELECT id FROM totals" \
  --column doubled --format tree
```

## Common Modifications

### Adding a new dialect
Dialects are handled by sqlglot - just use `--dialect name`. Check sqlglot docs for supported dialects.

### Adding new output format
1. Add format to argparse choices
2. Implement format function (e.g., `format_as_X()`)
3. Add case in `format_output()` or main

### Adding new script
1. Create `skills/sql-lineage/scripts/new_script.py`
2. Use PEP 723 header (copy from existing)
3. Follow pattern: `read_input()`, main logic, `argparse`, JSON output

## sqlglot Key APIs

```python
import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage
from sqlglot.optimizer.qualify import qualify

# Parse SQL to AST
ast = sqlglot.parse_one(sql, dialect=dialect)

# Trace column lineage
node = lineage(column_name, sql, dialect=dialect, schema=schema)

# Qualify column references with table names
qualified = qualify(ast, dialect=dialect, schema=schema)

# Find all nodes of type
for table in ast.find_all(exp.Table):
    print(table.name)
```

## Error Handling

Common errors:
- `ParseError` → Invalid SQL syntax, check dialect
- Column not in SELECT → Now searches CTEs automatically and returns location
- `Ambiguous column` → Provide schema for disambiguation

**CTE-aware behavior:** If a column isn't in the final SELECT but exists in a CTE, the tool returns success with the CTE location, expression, and source columns instead of failing.

## Git Guidelines
- **NEVER** run `git push`. All git operations should remain local.
- **NEVER** use interactive git commands (e.g., `git add -p`).
- Always run `git status` before committing.

## Documentation Maintenance

Keep documentation in sync with code changes:

| When you change... | Update these files |
|-------------------|-------------------|
| Script output format | `skills/sql-lineage/REFERENCE.md` |
| CLI arguments | `REFERENCE.md`, `README.md` |
| Skill behavior/usage | `skills/sql-lineage/SKILL.md` |
| Test coverage | `README.md` (Development section) |

**Before committing:** Run `uv run pytest tests/ -v` to verify all tests pass.

## Project Conventions (Lessons Learned)

- **Plugin folders use hyphens** (`sql-lineage`) per Claude Code spec - they cannot be Python packages
- **No `__init__.py` in hyphenated folders** - tests use `sys.path.append()` instead
- **Escape user content in HTML** - use `html.escape()` for HTML, `json.dumps()` for JS context
- **REFERENCE.md examples must match actual output** - run scripts and copy real output
