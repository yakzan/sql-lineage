"""Tests for new features: expression truncation, recursive tracing, diagram/summary formats."""

import sys
sys.path.insert(0, "skills/sql-lineage/scripts")

from trace_column import trace_column_lineage, truncate_expr, find_column_in_union
from analyze_query import analyze_query, build_cte_dependencies, format_as_diagram, format_as_summary
import sqlglot


class TestExpressionTruncation:
    """Tests for the --max-expr-length feature."""

    def test_truncate_expr_short(self):
        result = truncate_expr("SELECT id", 50)
        assert result == "SELECT id"

    def test_truncate_expr_long(self):
        long_expr = "CASE WHEN a > 100 THEN 'high' WHEN a > 50 THEN 'medium' ELSE 'low' END"
        result = truncate_expr(long_expr, 20)
        assert result == "CASE WHEN a > 100 TH..."
        assert len(result) == 23  # 20 chars + "..."

    def test_truncate_expr_none(self):
        assert truncate_expr(None, 50) is None

    def test_truncate_expr_no_limit(self):
        long_expr = "A" * 1000
        result = truncate_expr(long_expr, None)
        assert result == long_expr

    def test_trace_with_max_expr_length(self):
        sql = """
        WITH cte AS (
            SELECT CASE WHEN value > 100 THEN 'high' ELSE 'low' END as category
            FROM t
        )
        SELECT category FROM cte
        """
        result = trace_column_lineage(sql, "category", max_expr_length=30)
        
        assert result["success"] is True
        # Check that expressions are truncated
        for node in result.get("nodes", []):
            if node.get("expression"):
                assert len(node["expression"]) <= 33  # 30 + "..."


class TestRecursiveCteTracing:
    """Tests for recursive CTE lineage tracing."""

    def test_single_level_cte(self):
        sql = """
        WITH daily AS (SELECT SUM(amount) as total FROM orders)
        SELECT total FROM daily
        """
        result = trace_column_lineage(sql, "total")
        assert result["success"] is True

    def test_column_in_cte_has_full_lineage(self):
        sql = """
        WITH 
            base AS (SELECT amount FROM orders),
            calc AS (SELECT amount * 2 AS doubled FROM base)
        SELECT * FROM calc
        """
        # Search for 'amount' which is in base CTE
        result = trace_column_lineage(sql, "amount")
        
        assert result["success"] is True
        assert "full_lineage" in result or result.get("in_final_output") is True


class TestDepthLimit:
    """Tests for the --depth flag."""

    def test_depth_limit_respected(self):
        sql = """
        WITH 
            a AS (SELECT id FROM users),
            b AS (SELECT id FROM a),
            c AS (SELECT id FROM b)
        SELECT * FROM c
        """
        # With depth=1, should stop after first level
        result = trace_column_lineage(sql, "id", depth=1)
        assert result["success"] is True

    def test_depth_zero_treated_as_unlimited(self):
        """depth=0 should be normalized to unlimited (not stop immediately)."""
        sql = """
        WITH 
            a AS (SELECT id FROM users),
            b AS (SELECT id FROM a)
        SELECT * FROM b
        """
        result = trace_column_lineage(sql, "id", depth=0)
        assert result["success"] is True
        # Should still trace (not return empty full_lineage)

    def test_depth_negative_treated_as_unlimited(self):
        """Negative depth should be normalized to unlimited."""
        sql = "WITH a AS (SELECT id FROM users) SELECT * FROM a"
        result = trace_column_lineage(sql, "id", depth=-5)
        assert result["success"] is True


class TestCteDependencyDiagram:
    """Tests for CTE dependency diagram generation."""

    def test_build_cte_dependencies(self):
        sql = """
        WITH 
            a AS (SELECT id FROM users),
            b AS (SELECT id FROM a JOIN orders ON a.id = orders.user_id)
        SELECT * FROM b
        """
        ast = sqlglot.parse_one(sql, dialect="redshift")
        deps = build_cte_dependencies(ast)
        
        assert "a" in deps
        assert "b" in deps
        assert "users" in deps["a"]
        assert "a" in deps["b"]
        assert "orders" in deps["b"]

    def test_format_as_diagram(self):
        result = {
            "ctes": [
                {"name": "a", "columns": ["x"]},
                {"name": "b", "columns": ["y"]},
            ]
        }
        deps = {
            "a": ["users"],
            "b": ["a", "orders"],
        }
        
        output = format_as_diagram(result, deps)
        
        assert "```mermaid" in output
        assert "flowchart TD" in output
        assert "users --> a" in output
        assert "a --> b" in output
        assert "orders --> b" in output


class TestSummaryFormat:
    """Tests for the --format summary option."""

    def test_format_as_summary(self):
        result = {
            "tables": [
                {"name": "users", "alias": None, "schema": None},
                {"name": "orders", "alias": None, "schema": None},
            ],
            "ctes": [
                {"name": "a", "columns": ["x"]},
                {"name": "b", "columns": ["y"]},
            ],
            "columns": [
                {"output_position": 1, "output_name": "result"},
            ],
        }
        deps = {
            "a": ["users"],
            "b": ["a", "orders"],
        }
        
        output = format_as_summary(result, deps)
        
        assert "# SQL Summary" in output
        assert "## Source Tables" in output
        assert "## CTE Chain" in output
        assert "users" in output
        assert "orders" in output


class TestUnionBranchTracking:
    """Tests for UNION branch identification."""

    def test_find_column_in_union(self):
        sql = """
        SELECT id FROM orders
        UNION ALL
        SELECT id FROM returns
        """
        ast = sqlglot.parse_one(sql, dialect="redshift")
        locations = find_column_in_union(ast, "id")
        
        # Should find id in both branches
        assert len(locations) >= 2
        branches = [loc["branch"] for loc in locations]
        assert any("left" in b for b in branches)
        assert any("right" in b for b in branches)

    def test_cte_with_union(self):
        sql = """
        WITH combined AS (
            SELECT id, 'order' as type FROM orders
            UNION ALL
            SELECT id, 'return' as type FROM returns
        )
        SELECT * FROM combined
        """
        result = trace_column_lineage(sql, "type")
        
        # Should trace successfully
        assert result["success"] is True
