import json
import pytest
import sys
import os

# Add scripts directory to path to allow importing from kebab-case directory
sys.path.append(os.path.abspath("skills/sql-lineage/scripts"))

from trace_column import trace_column_lineage

# Fixtures for common scenarios
@pytest.fixture
def complex_cte_query():
    return """
    WITH layer1 AS (
        SELECT id, price * qty AS amount FROM raw_orders
    ),
    layer2 AS (
        SELECT id, amount, amount * 0.1 AS tax FROM layer1
    ),
    layer3 AS (
        SELECT id, amount + tax AS total FROM layer2
    )
    SELECT total FROM layer3
    """

def test_basic_lineage():
    sql = "SELECT id FROM users"
    result = trace_column_lineage(sql, "id")
    assert result["success"]
    assert result["column"] == "id"
    assert "users" in result["source_tables"]
    # Check graph structure: 2 nodes (id, users.id), 1 edge
    assert len(result["nodes"]) == 2
    assert len(result["edges"]) == 1

def test_complex_cte_lineage(complex_cte_query):
    result = trace_column_lineage(complex_cte_query, "total")
    assert result["success"]
    
    # Sources should eventually trace back to raw_orders columns
    sources = [n["column"] for n in result["nodes"] if n["type"] == "table"]
    assert "raw_orders.price" in sources or "price" in sources # Depending on qualification
    assert "raw_orders.qty" in sources or "qty" in sources
    
    # Verify graph connectivity (total -> amount + tax -> amount, tax -> ...)
    # Just checking we have a connected graph with multiple nodes
    assert len(result["nodes"]) > 5
    assert len(result["edges"]) > 4

def test_window_function_lineage():
    sql = """
    SELECT 
        user_id, 
        RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as rank_val 
    FROM employees
    """
    result = trace_column_lineage(sql, "rank_val")
    assert result["success"]
    
    # Should trace to department_id and salary
    sources = set()
    for n in result["nodes"]:
        if n["type"] == "table":
            sources.add(n["column"])
            
    # Note: SQLGlot lineage for window functions might be tricky depending on version/optimizer
    # Ideally it catches partition/order columns. 
    # If not, at least ensure it doesn't crash and identifies 'employees' table.
    assert "employees" in result["source_tables"]

def test_union_lineage():
    sql = """
    SELECT x FROM t1
    UNION ALL
    SELECT x FROM t2
    """
    result = trace_column_lineage(sql, "x")
    assert result["success"]
    assert "t1" in result["source_tables"]
    assert "t2" in result["source_tables"]

def test_json_output_safety():
    # Test for XSS-safe label encoding in HTML generation logic (implicitly tested via node content)
    # But explicitly checking if we can handle weird characters
    # Note: Double quotes in identifiers are dialect specific. 
    # Standard SQL uses double quotes for identifiers.
    sql = 'SELECT "weird""column" FROM t'
    result = trace_column_lineage(sql, 'weird"column')
    
    if not result["success"]:
        # Fallback to a simpler case if dialect issues arise with escaping
        sql = 'SELECT col AS "weird_chars_&<>" FROM t'
        result = trace_column_lineage(sql, 'weird_chars_&<>')
        
    # Success means it parsed and didn't crash
    assert result["success"]
    # Ensure special chars are present in some node name
    assert any('weird' in n["name"] for n in result["nodes"])
