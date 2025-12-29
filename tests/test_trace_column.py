#!/usr/bin/env python3
"""Tests for trace_column.py with CTE-aware column tracing."""

import json
import subprocess
import sys
from pathlib import Path

# Path to the script
SCRIPT_PATH = Path(__file__).parent.parent / "skills" / "sql-lineage" / "scripts" / "trace_column.py"


def run_trace(sql: str, column: str, format: str = "json", dialect: str = None) -> dict:
    """Run trace_column.py and return parsed result."""
    # Use the active Python interpreter to avoid uv cache/permission issues in CI or sandboxes.
    cmd = [sys.executable, str(SCRIPT_PATH), sql, "--column", column, "--format", format]
    if dialect:
        cmd.extend(["--dialect", dialect])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if format == "json":
        return json.loads(result.stdout)
    return {"stdout": result.stdout, "returncode": result.returncode}


class TestColumnInFinalOutput:
    """Tests for columns in final SELECT output."""

    def test_simple_column(self):
        result = run_trace("SELECT id FROM users", "id")
        assert result["success"] is True
        assert result["column"] == "id"
        assert result.get("in_final_output") is True
        assert "nodes" in result

    def test_aliased_column(self):
        result = run_trace("SELECT id AS user_id FROM users", "user_id")
        assert result["success"] is True
        assert result["column"] == "user_id"
        assert result.get("in_final_output") is True

    def test_column_from_subquery(self):
        sql = "SELECT a FROM (SELECT x AS a FROM t) sub"
        result = run_trace(sql, "a")
        assert result["success"] is True
        assert result.get("in_final_output") is True

    def test_column_in_both_final_and_cte(self):
        """Column in both final output and CTE should prioritize final output."""
        sql = """
        WITH totals AS (
            SELECT id, amount AS total FROM orders
        )
        SELECT total FROM totals
        """
        result = run_trace(sql, "total")
        assert result["success"] is True
        assert result.get("in_final_output") is True
        # Should have full lineage nodes/edges, not CTE-found response
        assert "nodes" in result
        assert "edges" in result


class TestColumnInCTE:
    """Tests for columns defined in CTEs but not in final output."""

    def test_column_in_single_cte(self):
        sql = """
        WITH totals AS (
            SELECT id, amount * 2 AS doubled
            FROM orders
        )
        SELECT id FROM totals
        """
        result = run_trace(sql, "doubled")
        assert result["success"] is True
        assert result["column"] == "doubled"
        assert result.get("in_final_output") is False
        assert "found_in" in result
        assert len(result["found_in"]) == 1
        assert result["found_in"][0]["cte_name"] == "totals"

    def test_column_in_multiple_ctes(self):
        sql = """
        WITH cte1 AS (
            SELECT id, amount AS total FROM orders
        ),
        cte2 AS (
            SELECT id, amount AS total FROM refunds
        )
        SELECT * FROM cte1
        """
        result = run_trace(sql, "total")
        assert result["success"] is True
        assert result.get("in_final_output") is False
        assert len(result["found_in"]) == 2
        cte_names = [loc["cte_name"] for loc in result["found_in"]]
        assert "cte1" in cte_names
        assert "cte2" in cte_names

    def test_cte_column_case_insensitive(self):
        sql = """
        WITH summary AS (
            SELECT COUNT(*) AS TotalCount FROM items
        )
        SELECT 1
        """
        result = run_trace(sql, "totalcount")  # lowercase
        assert result["success"] is True
        assert result.get("in_final_output") is False
        assert len(result["found_in"]) == 1

    def test_cte_response_includes_available_ctes(self):
        """CTE-found response should include available_ctes for agent recursion."""
        sql = """
        WITH cte1 AS (SELECT id FROM users),
             cte2 AS (SELECT cte1.id AS user_id FROM cte1)
        SELECT 1
        """
        result = run_trace(sql, "user_id")
        assert result["success"] is True
        assert result.get("in_final_output") is False
        assert "available_ctes" in result
        assert "cte1" in result["available_ctes"]
        assert "cte2" in result["available_ctes"]
        # Sources should indicate this comes from cte1
        assert any("cte1" in s for s in result["found_in"][0]["sources"])

    def test_sources_are_deduplicated(self):
        """Sources should not have duplicates when column referenced multiple times."""
        sql = """
        WITH calc AS (
            SELECT a + a + a AS triple FROM t
        )
        SELECT 1
        """
        result = run_trace(sql, "triple")
        assert result["success"] is True
        sources = result["found_in"][0]["sources"]
        # Even though 'a' is referenced 3 times, should appear only once
        assert len(sources) == len(set(sources))


class TestColumnNotFound:
    """Tests for columns that don't exist anywhere."""

    def test_nonexistent_column(self):
        result = run_trace("SELECT id FROM users", "nonexistent")
        assert result["success"] is False
        assert "error" in result
        assert "available_in_output" in result
        assert "id" in result["available_in_output"]

    def test_lists_available_ctes(self):
        sql = """
        WITH my_cte AS (SELECT 1 AS x)
        SELECT * FROM my_cte
        """
        result = run_trace(sql, "nonexistent")
        assert result["success"] is False
        assert "available_ctes" in result
        assert "my_cte" in result["available_ctes"]


class TestDialectDefault:
    """Tests for default Redshift dialect."""

    def test_redshift_functions_parse(self):
        # NVL and :: casting are Redshift-specific
        sql = "SELECT nvl(a, 0) AS result, b::int FROM t"
        result = run_trace(sql, "result")
        # Should not error - default dialect handles Redshift syntax
        assert result["success"] is True or "Parse error" not in result.get("error", "")


class TestOutputFormats:
    """Tests for different output formats."""

    def test_tree_format_cte_column(self):
        sql = """
        WITH calc AS (SELECT 1 + 1 AS sum_result)
        SELECT 1
        """
        result = run_trace(sql, "sum_result", format="tree")
        assert "CTE: calc" in result["stdout"]
        assert "Expression:" in result["stdout"]

    def test_tree_format_not_found(self):
        result = run_trace("SELECT id FROM users", "missing", format="tree")
        assert "Error:" in result["stdout"]
        assert "Available columns:" in result["stdout"]


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
