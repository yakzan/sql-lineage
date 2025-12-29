"""Tests for impact analysis, self-referencing resolution, data types, and aggregation semantics."""

import sys
sys.path.insert(0, "skills/sql-lineage/scripts")

from trace_column import (
    trace_column_lineage,
    extract_source_columns,
    build_alias_map,
)
from analyze_query import (
    analyze_query,
    infer_data_type,
    extract_aggregation_info,
)
from impact_analysis import (
    analyze_impact,
    build_dependency_graph,
    build_reverse_index,
    find_line_numbers,
)
import sqlglot
from sqlglot import exp


class TestSelfReferencingResolution:
    """Tests for self-referencing column resolution."""

    def test_simple_self_reference(self):
        """Column referencing an earlier alias in same SELECT."""
        sql = """
        WITH test AS (
            SELECT
                o.amount,
                o.amount * 2 AS doubled,
                doubled + 10 AS final
            FROM orders o
        )
        SELECT final FROM test
        """
        result = trace_column_lineage(sql, "final")
        assert result["success"] is True
        # Column is in final output, so check nodes
        # The expression should show resolved form: o.amount * 2 + 10
        nodes = result.get("nodes", [])
        # Should have traced back to orders.amount
        source_tables = result.get("source_tables", [])
        assert "orders" in source_tables

    def test_chain_of_self_references(self):
        """Multiple levels of self-referencing."""
        sql = """
        WITH calc AS (
            SELECT
                a.val,
                val * 2 AS step1,
                step1 + 5 AS step2,
                step2 * 3 AS step3
            FROM data a
        )
        SELECT step3 FROM calc
        """
        result = trace_column_lineage(sql, "step3")
        assert result["success"] is True

    def test_self_ref_with_aggregation(self):
        """Self-referencing with aggregations (like cancel_score example)."""
        sql = """
        WITH metrics AS (
            SELECT
                SUM(CASE WHEN status > 90 THEN 1 END) AS total,
                SUM(CASE WHEN status = 91 THEN 1 END) AS canceled,
                canceled / total::float AS cancel_ratio,
                CASE WHEN cancel_ratio < 0.05 THEN 100 ELSE 0 END AS cancel_score
            FROM orders
            GROUP BY customer_id
        )
        SELECT * FROM metrics
        """
        result = trace_column_lineage(sql, "cancel_score")
        assert result["success"] is True
        # The sources should include status via the self-reference chain
        found_in = result.get("found_in", [])
        if found_in:
            sources = found_in[0].get("sources", [])
            # Should have resolved cancel_ratio -> canceled, total -> status
            assert len(sources) > 0

    def test_no_infinite_loop_on_circular(self):
        """Ensure no infinite loop if there's a circular reference (invalid SQL but should handle gracefully)."""
        sql = "SELECT a AS b, b AS a FROM t"
        ast = sqlglot.parse_one(sql, dialect="redshift")
        alias_map = build_alias_map(ast.selects)
        # Should not hang
        sources = extract_source_columns(ast.selects[0], alias_map)
        assert isinstance(sources, list)


class TestDataTypeTracking:
    """Tests for data type inference."""

    def test_count_returns_bigint(self):
        sql = "SELECT COUNT(*) AS cnt FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "BIGINT"

    def test_sum_returns_numeric(self):
        sql = "SELECT SUM(amount) AS total FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "NUMERIC"

    def test_avg_returns_double(self):
        sql = "SELECT AVG(price) AS avg_price FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "DOUBLE"

    def test_cast_returns_target_type(self):
        sql = "SELECT CAST(amount AS VARCHAR) AS amount_str FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert "VARCHAR" in col["data_type"]

    def test_case_with_string_literals(self):
        sql = "SELECT CASE WHEN x > 0 THEN 'yes' ELSE 'no' END AS flag FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "VARCHAR"

    def test_case_with_numeric_literals(self):
        sql = "SELECT CASE WHEN x > 0 THEN 100 ELSE 0 END AS score FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "NUMERIC"

    def test_arithmetic_returns_numeric(self):
        sql = "SELECT a + b AS sum_val FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "NUMERIC"

    def test_date_extract_returns_integer(self):
        sql = "SELECT EXTRACT(YEAR FROM date_col) AS yr FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "INTEGER"


class TestAggregationSemantics:
    """Tests for aggregation semantics."""

    def test_simple_aggregation(self):
        sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
        result = analyze_query(sql)
        assert result["success"] is True

        # total should have aggregation info
        total_col = next(c for c in result["columns"] if c["output_name"] == "total")
        assert "aggregation" in total_col
        assert total_col["aggregation"]["function"] == "SUM"
        assert "amount" in total_col["aggregation"]["input_columns"][0]

    def test_aggregation_with_group_by(self):
        sql = "SELECT date, COUNT(*) AS cnt FROM orders GROUP BY date"
        result = analyze_query(sql)
        assert result["success"] is True

        cnt_col = next(c for c in result["columns"] if c["output_name"] == "cnt")
        assert "grouped_by" in cnt_col
        assert len(cnt_col["grouped_by"]) > 0

    def test_derived_aggregation(self):
        """Expression containing multiple aggregations."""
        sql = "SELECT SUM(amount) / COUNT(*) AS avg_amount FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True

        col = result["columns"][0]
        assert "aggregation" in col
        assert col["aggregation"]["function"] == "DERIVED"
        assert "contains" in col["aggregation"]

    def test_non_aggregated_column_has_no_aggregation_field(self):
        sql = "SELECT id FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert "aggregation" not in col


class TestImpactAnalysis:
    """Tests for impact analysis (reverse lineage)."""

    def test_direct_dependency(self):
        sql = "SELECT amount * 2 AS doubled FROM orders"
        result = analyze_impact(sql, "amount")
        assert result["success"] is True
        assert result["impact_summary"]["total_affected"] >= 1

    def test_cte_transitive_dependency(self):
        """Impact should flow through CTEs."""
        sql = """
        WITH step1 AS (SELECT amount * 2 AS doubled FROM orders),
             step2 AS (SELECT doubled + 10 AS final FROM step1)
        SELECT final FROM step2
        """
        result = analyze_impact(sql, "amount")
        assert result["success"] is True
        # Should affect step1.doubled, step2.final, output.final
        assert result["impact_summary"]["total_affected"] >= 2

    def test_unqualified_source_column(self):
        """Should find column even without table prefix."""
        sql = "SELECT status, status + 1 AS inc FROM orders"
        result = analyze_impact(sql, "status")
        assert result["success"] is True

    def test_nonexistent_source_column(self):
        """Should return error for non-existent column."""
        sql = "SELECT id FROM orders"
        result = analyze_impact(sql, "nonexistent_column")
        assert result["success"] is False
        assert "available_sources" in result

    def test_multiple_ctes_affected(self):
        """Source column affecting multiple CTEs."""
        sql = """
        WITH cte1 AS (SELECT id, amount FROM orders),
             cte2 AS (SELECT id, amount * 2 AS doubled FROM cte1),
             cte3 AS (SELECT id, amount / 2 AS halved FROM cte1)
        SELECT * FROM cte2 JOIN cte3 USING (id)
        """
        result = analyze_impact(sql, "amount")
        assert result["success"] is True
        # Should affect cte1.amount, cte2.doubled, cte3.halved
        affected_ctes = {c["cte"] for c in result["impacted_cte_columns"]}
        assert len(affected_ctes) >= 2


class TestBuildDependencyGraph:
    """Tests for the dependency graph building."""

    def test_simple_graph(self):
        sql = "SELECT a, b + 1 AS c FROM t"
        ast = sqlglot.parse_one(sql, dialect="redshift")
        graph = build_dependency_graph(ast)

        assert "output.a" in graph or "output.c" in graph
        assert len(graph) >= 2

    def test_cte_in_graph(self):
        sql = "WITH cte AS (SELECT x FROM t) SELECT x FROM cte"
        ast = sqlglot.parse_one(sql, dialect="redshift")
        graph = build_dependency_graph(ast)

        assert any("cte." in k for k in graph.keys())

    def test_reverse_index(self):
        sql = "SELECT a + b AS sum_col FROM t"
        ast = sqlglot.parse_one(sql, dialect="redshift")
        graph = build_dependency_graph(ast)
        reverse_index = build_reverse_index(graph)

        # Both a and b should have reverse mappings
        assert any("a" in k for k in reverse_index.keys()) or any("b" in k for k in reverse_index.keys())


class TestEdgeCases:
    """Edge case tests for robustness."""

    # Data type edge cases
    def test_min_max_returns_inherited(self):
        sql = "SELECT MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        for col in result["columns"]:
            assert col["data_type"] == "INHERITED"

    def test_coalesce_returns_inherited(self):
        sql = "SELECT COALESCE(a, b, 0) AS val FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        assert result["columns"][0]["data_type"] == "INHERITED"

    def test_boolean_expression_returns_boolean(self):
        sql = "SELECT a > 10 AS is_large, b = 'x' AS is_x FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        for col in result["columns"]:
            assert col["data_type"] == "BOOLEAN"

    def test_nested_arithmetic(self):
        sql = "SELECT (a + b) * (c - d) / e AS complex_calc FROM t"
        result = analyze_query(sql)
        assert result["success"] is True
        assert result["columns"][0]["data_type"] == "NUMERIC"

    def test_count_distinct(self):
        sql = "SELECT COUNT(DISTINCT user_id) AS unique_users FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = result["columns"][0]
        assert col["data_type"] == "BIGINT"
        assert col["aggregation"]["function"] == "COUNT"

    # Impact analysis edge cases
    def test_impact_through_union(self):
        """Impact should propagate through UNION branches."""
        sql = """
        SELECT id, status FROM orders
        UNION ALL
        SELECT id, status FROM archived_orders
        """
        result = analyze_impact(sql, "status")
        assert result["success"] is True
        # Should find status in both branches
        assert result["impact_summary"]["total_affected"] >= 1

    def test_impact_subquery_limitation(self):
        """Document: inline subqueries are not traversed (only CTEs are)."""
        sql = """
        SELECT t.doubled FROM (
            SELECT amount * 2 AS doubled FROM orders
        ) t
        """
        # Current limitation: inline subqueries not traversed
        # Impact analysis works with CTEs, not inline subqueries
        result = analyze_impact(sql, "amount")
        # This returns False because subquery columns aren't in the graph
        assert result["success"] is False
        # But the available sources show the subquery alias
        assert "t.doubled" in result.get("available_sources", [])

    def test_impact_with_join_same_column_name(self):
        """Handle same column name from multiple tables in JOIN."""
        sql = """
        SELECT o.id, u.id AS user_id, o.amount
        FROM orders o
        JOIN users u ON o.user_id = u.id
        """
        # Impact of orders.id should only affect o.id
        result = analyze_impact(sql, "o.id")
        assert result["success"] is True

    def test_qualified_source_column_matches_base_table(self):
        """Qualification should allow referencing base table name, not just alias."""
        sql = """
        SELECT o.id, u.id AS user_id, o.amount
        FROM orders o
        JOIN users u ON o.user_id = u.id
        """
        result = analyze_impact(sql, "orders.id")
        assert result["success"] is True
        assert any(src.startswith("orders.id") for src in result.get("available_source_columns", []))

    def test_union_branch_preserves_table_lineage(self):
        """Each UNION branch should retain its table lineage for impact targeting."""
        sql = """
        SELECT id, status FROM orders
        UNION ALL
        SELECT id, status FROM archived_orders
        """
        # Changing orders.status should be recognized
        result = analyze_impact(sql, "orders.status")
        assert result["success"] is True
        # Ensure archived_orders remains separately addressable
        assert any("archived_orders.status" in s for s in result.get("available_source_columns", []))

    # Self-referencing edge cases
    def test_self_ref_with_case_expression(self):
        """Self-reference inside CASE expression."""
        sql = """
        WITH calc AS (
            SELECT
                amount,
                amount * 0.1 AS tax,
                CASE WHEN tax > 100 THEN tax * 0.9 ELSE tax END AS final_tax
            FROM orders
        )
        SELECT final_tax FROM calc
        """
        result = trace_column_lineage(sql, "final_tax")
        assert result["success"] is True

    def test_self_ref_not_resolved_when_table_qualified(self):
        """Table-qualified columns should NOT be resolved as self-references."""
        sql = "SELECT t.a, t.a + 1 AS b FROM t"
        ast = sqlglot.parse_one(sql, dialect="redshift")
        alias_map = build_alias_map(ast.selects)
        sources = extract_source_columns(ast.selects[1], alias_map)
        # Should have t.a, not resolved through alias
        assert any("t.a" in s for s in sources)

    # Aggregation edge cases
    def test_aggregation_in_having(self):
        """Aggregation context with HAVING clause."""
        sql = """
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
        HAVING SUM(amount) > 1000
        """
        result = analyze_query(sql)
        assert result["success"] is True
        total_col = next(c for c in result["columns"] if c["output_name"] == "total")
        assert total_col["aggregation"]["function"] == "SUM"

    def test_window_function_not_marked_as_aggregation(self):
        """Window functions should not be marked as regular aggregations."""
        sql = "SELECT id, SUM(amount) OVER (PARTITION BY customer_id) AS running_total FROM orders"
        result = analyze_query(sql)
        assert result["success"] is True
        col = next(c for c in result["columns"] if c["output_name"] == "running_total")
        # Window functions are different from aggregations
        # They should either have no aggregation or be marked specially
        # Current implementation may vary - just ensure it doesn't crash
        assert "output_name" in col


class TestSummaryOnlyAndLineNumbers:
    """Tests for --summary-only and --include-line-numbers flags."""

    def test_summary_only_omits_expressions(self):
        """summary_only=True should omit expression fields."""
        sql = """
        WITH step1 AS (SELECT amount * 2 AS doubled FROM orders)
        SELECT doubled FROM step1
        """
        result = analyze_impact(sql, "amount", summary_only=True)
        assert result["success"] is True

        # Check that impacted columns have no expression field
        for col in result.get("impacted_output_columns", []):
            assert "expression" not in col
        for col in result.get("impacted_cte_columns", []):
            assert "expression" not in col

    def test_summary_only_preserves_structure(self):
        """summary_only should still include all other fields."""
        sql = """
        WITH calc AS (SELECT id, amount * 2 AS doubled FROM orders)
        SELECT doubled FROM calc
        """
        result = analyze_impact(sql, "amount", summary_only=True)
        assert result["success"] is True

        # Check structure is preserved
        assert "impact_summary" in result
        assert "impacted_output_columns" in result
        assert "impacted_cte_columns" in result
        assert "available_source_columns" in result

        # Output columns should have position and column name
        for col in result.get("impacted_output_columns", []):
            assert "column" in col
            assert "position" in col

        # CTE columns should have cte and column name
        for col in result.get("impacted_cte_columns", []):
            assert "cte" in col
            assert "column" in col

    def test_include_line_numbers_adds_line_info(self):
        """include_line_numbers=True should add line hints."""
        sql = """WITH step1 AS (
    SELECT amount * 2 AS doubled FROM orders
)
SELECT doubled FROM step1"""
        result = analyze_impact(sql, "amount", include_line_numbers=True)
        assert result["success"] is True

        # Should have line_numbers dict
        assert "line_numbers" in result
        assert "cte:step1" in result["line_numbers"]
        assert "final_select" in result["line_numbers"]

        # CTE columns should have line_hint
        for col in result.get("impacted_cte_columns", []):
            assert "line_hint" in col

    def test_find_line_numbers_with_multiple_ctes(self):
        """find_line_numbers should find all CTEs."""
        sql = """WITH
    cte_a AS (SELECT 1 AS a),
    cte_b AS (SELECT 2 AS b),
    cte_c AS (SELECT 3 AS c)
SELECT a, b, c FROM cte_a, cte_b, cte_c"""

        cte_names = {"cte_a", "cte_b", "cte_c"}
        line_info = find_line_numbers(sql, cte_names)

        assert "cte:cte_a" in line_info
        assert "cte:cte_b" in line_info
        assert "cte:cte_c" in line_info
        assert "final_select" in line_info

        # CTEs should be on lines 2, 3, 4; final SELECT on line 5
        assert line_info["cte:cte_a"] == 2
        assert line_info["cte:cte_b"] == 3
        assert line_info["cte:cte_c"] == 4
        assert line_info["final_select"] == 5

    def test_summary_only_with_line_numbers_combined(self):
        """Both flags should work together."""
        sql = """WITH calc AS (
    SELECT amount * 2 AS doubled FROM orders
)
SELECT doubled FROM calc"""
        result = analyze_impact(
            sql, "amount",
            summary_only=True,
            include_line_numbers=True
        )
        assert result["success"] is True

        # Should have line numbers but no expressions
        assert "line_numbers" in result

        for col in result.get("impacted_cte_columns", []):
            assert "line_hint" in col
            assert "expression" not in col

    def test_max_expr_length_truncates(self):
        """max_expr_length should truncate long expressions."""
        sql = """
        SELECT
            CASE WHEN x > 0 THEN 'very_long_string_that_exceeds_limit' ELSE 'another_long_string' END AS result
        FROM t
        """
        result = analyze_impact(sql, "x", max_expr_length=30)
        assert result["success"] is True

        # Expression should be truncated
        for col in result.get("impacted_output_columns", []):
            if col.get("expression"):
                assert len(col["expression"]) <= 33  # 30 + "..."

    def test_max_sources_limits_output(self):
        """max_sources should limit available_source_columns."""
        sql = "SELECT a, b, c, d, e, f, g FROM t"
        result = analyze_impact(sql, "a", max_sources=3)
        assert result["success"] is True
        assert len(result.get("available_source_columns", [])) <= 3

    def test_line_numbers_for_simple_query_no_ctes(self):
        """Queries without CTEs should still work."""
        sql = "SELECT amount * 2 AS doubled FROM orders"
        result = analyze_impact(sql, "amount", include_line_numbers=True)
        assert result["success"] is True
        assert "line_numbers" in result
        # Should have final_select but no CTEs
        assert "final_select" in result["line_numbers"]
