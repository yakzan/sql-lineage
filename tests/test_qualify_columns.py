import pytest
import sys
import os

# Add scripts directory to path to allow importing from kebab-case directory
sys.path.append(os.path.abspath("skills/sql-lineage/scripts"))

from qualify_columns import qualify_query


def test_basic_qualification():
    sql = "SELECT id, name FROM users"
    schema = {"users": {"id": "INT", "name": "VARCHAR"}}
    result = qualify_query(sql, schema=schema)

    assert result["success"]
    # Qualified SQL should have table prefixes
    assert "users.id" in result["qualified"]
    assert "users.name" in result["qualified"]


def test_join_qualification():
    sql = "SELECT id, amount FROM users JOIN orders ON user_id = id"
    schema = {
        "users": {"id": "INT", "name": "VARCHAR"},
        "orders": {"order_id": "INT", "user_id": "INT", "amount": "DECIMAL"},
    }
    result = qualify_query(sql, schema=schema)

    assert result["success"]
    # Verify ambiguous columns are properly qualified with table prefixes
    assert "users.id" in result["qualified"]
    assert "orders.amount" in result["qualified"]
    # Verify join condition is qualified
    assert "orders.user_id" in result["qualified"]


def test_without_schema():
    # Without schema, qualification should still work but with validate_qualify_columns=False
    sql = "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id"
    result = qualify_query(sql)

    assert result["success"]
    assert "qualified" in result


def test_invalid_sql():
    # Truly malformed SQL that can't be parsed
    sql = "SELECT FROM WHERE (((("
    result = qualify_query(sql)

    assert not result["success"]
    assert "error" in result


def test_dialect_specific():
    # BigQuery uses backticks for identifiers
    sql = "SELECT id FROM `project.dataset.table`"
    result = qualify_query(sql, dialect="bigquery")

    assert result["success"]
