# AGENTS.md

This project uses Claude Code conventions. All AI agents should read and follow:

**→ [CLAUDE.md](CLAUDE.md)** - Project conventions, architecture, and guidelines

## Quick Reference

- **Python scripts**: Use `uv run script.py` (PEP 723 inline dependencies)
- **Run tests**: `uv run pytest tests/ -v`
- **Update docs**: When changing functionality, update corresponding `.md` files

## Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Full project conventions and architecture |
| `skills/sql-lineage/SKILL.md` | Skill usage instructions |
| `skills/sql-lineage/REFERENCE.md` | Detailed API documentation |
| `README.md` | User-facing documentation |
