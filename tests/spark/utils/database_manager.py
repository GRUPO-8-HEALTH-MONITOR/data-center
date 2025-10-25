import os
import mysql.connector as mysql

class MySQLDatabase:
    def __init__(self):
        self._host = os.getenv('HOST')
        self._username = os.getenv('USERNAME')
        self._password = os.getenv('PASSWORD')
        self._database = os.getenv('DATABASE')
        
        try:
            self.conn = self._connecting()
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            
        except mysql.Error as e:
            raise ConnectionError(f"Error connecting to MySQL: {e}") from e


    def _connecting(self):
        return mysql.connect(
            user=self._username,
            password=self._password,
            host=self._host,
            database=self._database,
        )