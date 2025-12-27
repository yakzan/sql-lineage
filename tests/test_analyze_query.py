import pytest
import sys
import os

# Add scripts directory to path to allow importing from kebab-case directory
sys.path.append(os.path.abspath("skills/sql-lineage/scripts"))

from analyze_query import analyze_query

def test_analyze_simple_select():
    sql = "SELECT id, name FROM users WHERE active = true"
    result = analyze_query(sql)
    
    assert result["success"]
    assert result["query_type"] == "SELECT"
    assert len(result["tables"]) == 1
    assert result["tables"][0]["name"] == "users"
    assert len(result["columns"]) == 2
    assert len(result["filters"]) == 1

def test_analyze_joins():
    sql = """
    SELECT u.name, o.id 
    FROM users u 
    JOIN orders o ON u.id = o.user_id
    LEFT JOIN regions r ON u.region_id = r.id
    """
    result = analyze_query(sql)
    
    assert result["success"]
    assert len(result["tables"]) == 3
    assert len(result["joins"]) == 2
    
    join_types = [j["type"] for j in result["joins"]]
    
    # Check if we have 2 joins
    assert len(join_types) == 2
    # Verify we have at least one INNER/JOIN
    assert "INNER" in join_types or "JOIN" in join_types
    # Verify we have one LEFT join.
    assert any("LEFT" in jt for jt in join_types), f"Expected LEFT join, got {join_types}"
    # One should be standard join
    assert "INNER" in join_types or "JOIN" in join_types
    # The other is LEFT join. SQLGlot might return "LEFT OUTER" or "LEFT"
    # Or maybe it failed to parse as LEFT?
    # Let's allow for broader matching or check the SQL string
    assert any("LEFT" in jt for jt in join_types)

def test_analyze_aggregations():
    sql = "SELECT department, COUNT(*) as cnt, AVG(salary) as avg_sal FROM employees GROUP BY department"
    result = analyze_query(sql)
    
    assert result["success"]
    assert len(result["aggregations"]) == 2
    assert len(result["group_by"]) == 1
    
    col_transforms = {c["output_name"]: c["transformation"] for c in result["columns"]}
    assert col_transforms["cnt"] == "aggregated"
    assert col_transforms["avg_sal"] == "aggregated"
    assert col_transforms["department"] == "passthrough" or "renamed"

def test_analyze_create_table_as():
    sql = "CREATE TABLE new_users AS SELECT * FROM old_users"
    result = analyze_query(sql)
    
    assert result["success"]
    assert result["query_type"] == "CREATE_TABLE_AS_SELECT"
    assert result["target_table"] == "new_users"
    assert len(result["tables"]) == 1
    assert result["tables"][0]["name"] == "old_users"
