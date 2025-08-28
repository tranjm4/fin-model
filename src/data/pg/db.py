"""
File: data/pg/db.py

This file handles the interaction with the PostgreSQL database.
"""
import psycopg2
from typing import Any, Optional, Dict

class PostgresDBWrapper:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.connection = psycopg2.connect(self.db_url)

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Executes an SQL query on the database and returns the result.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def close(self):
        self.connection.close()