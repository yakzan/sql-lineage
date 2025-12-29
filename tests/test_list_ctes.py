"""Tests for list_ctes.py script."""

import sys
sys.path.insert(0, "skills/sql-lineage/scripts")

from list_ctes import list_ctes, extract_cte_info


class TestListCtes:
    """Tests for listing CTEs in a query."""

    def test_single_cte(self):
        sql = "WITH a AS (SELECT 1 AS x, 2 AS y) SELECT * FROM a"
        result = list_ctes(sql)
        
        assert result["success"] is True
        assert result["cte_count"] == 1
        assert len(result["ctes"]) == 1
        assert result["ctes"][0]["name"] == "a"
        assert set(result["ctes"][0]["columns"]) == {"x", "y"}

    def test_multiple_ctes(self):
        sql = """
        WITH 
            a AS (SELECT 1 AS x FROM users),
            b AS (SELECT x FROM a)
        SELECT * FROM b
        """
        result = list_ctes(sql)
        
        assert result["success"] is True
        assert result["cte_count"] == 2
        
        cte_names = [c["name"] for c in result["ctes"]]
        assert "a" in cte_names
        assert "b" in cte_names

    def test_cte_references(self):
        sql = """
        WITH 
            a AS (SELECT id FROM users),
            b AS (SELECT id FROM a JOIN orders ON a.id = orders.user_id)
        SELECT * FROM b
        """
        result = list_ctes(sql)
        
        assert result["success"] is True
        
        # Find CTE b
        cte_b = next(c for c in result["ctes"] if c["name"] == "b")
        assert "a" in cte_b["references"]
        assert "orders" in cte_b["references"]

    def test_no_ctes(self):
        sql = "SELECT id FROM users"
        result = list_ctes(sql)
        
        assert result["success"] is True
        assert result["cte_count"] == 0
        assert result["ctes"] == []

    def test_parse_error(self):
        sql = "SELECT * FROM"  # Invalid SQL
        result = list_ctes(sql)
        
        assert result["success"] is False
        assert "error" in result
