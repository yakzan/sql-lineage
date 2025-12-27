import pytest
import sys
import os

# Add scripts directory to path to allow importing from kebab-case directory
sys.path.append(os.path.abspath("skills/sql-lineage/scripts"))

from extract_tables import extract_tables

def test_extract_basic():
    sql = "SELECT * FROM users"
    result = extract_tables(sql)
    assert result["success"]
    assert len(result["tables"]) == 1
    assert result["tables"][0]["name"] == "users"

def test_extract_deduplication():
    # P2 fix verification
    sql = """
    SELECT * 
    FROM users u1
    JOIN users u2 ON u1.manager_id = u2.id
    """
    result = extract_tables(sql)
    assert result["success"]
    
    # Should have 2 entries because aliases are different, but same table name
    # Wait, the fix was to deduplicate by the entire table_info dict.
    # If aliases are different, they are different entries?
    # Let's check logic:
    # table_info includes 'alias'. So if aliases differ, they are kept.
    # If we have exact duplicate (same table, same alias/no alias), it should be deduped.
    
    assert len(result["tables"]) == 2
    names = [t["name"] for t in result["tables"]]
    aliases = [t["alias"] for t in result["tables"]]
    assert names == ["users", "users"]
    assert "u1" in aliases
    assert "u2" in aliases

def test_extract_exact_duplicate():
    # This should be deduplicated
    sql = "SELECT * FROM t JOIN t" # Implicit join or error, but valid SQL AST
    # Or valid: SELECT * FROM t, t
    sql = "SELECT * FROM t, t" 
    result = extract_tables(sql)
    
    # Both have name='t', alias=None. Should be 1 entry.
    assert len(result["tables"]) == 1
    assert result["tables"][0]["name"] == "t"

def test_extract_cte_awareness():
    # Extract tables should typically NOT include CTE names as tables if they are defined internally
    # But sqlglot find_all(exp.Table) might return CTE references as tables depending on scope.
    # Let's see behavior. Ideally we want physical tables.
    sql = "WITH cte AS (SELECT * FROM raw_table) SELECT * FROM cte"
    result = extract_tables(sql)
    
    names = [t["name"] for t in result["tables"]]
    # Ideally 'raw_table' is there. 'cte' might be there depending on implementation.
    assert "raw_table" in names
