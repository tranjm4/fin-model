import pytest
from unittest.mock import Mock, MagicMock, patch
from dotenv import load_dotenv
import os
from src.data.pg.db import PostgresDBWrapper

load_dotenv()

def test_db_wrapper_connection():
    db_wrapper = PostgresDBWrapper(os.getenv("PG_URL"))
    assert db_wrapper.connection is not None

def test_db_wrapper_execute_query():
    db_wrapper = Mock(spec=PostgresDBWrapper)
    db_wrapper.connection = Mock()
    db_wrapper.connection.cursor = MagicMock()
    db_wrapper.connection.cursor.return_value.__enter__.return_value.fetchall.return_value = [(1, 'test')]
    db_wrapper.execute_query.return_value = [(1, 'test')]
    result = db_wrapper.execute_query("SELECT * FROM test_table")

    assert result == [(1, 'test')] # Ensure execute_query uses cursor.fetchall
    
def test_db_wrapper_close():
    db_wrapper = PostgresDBWrapper(os.getenv("PG_URL"))
    db_wrapper.connection = Mock()
    db_wrapper.close()
    db_wrapper.connection.close.assert_called_once()