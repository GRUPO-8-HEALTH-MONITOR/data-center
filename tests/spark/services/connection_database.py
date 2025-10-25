import mysql.connector
from mysql.connector import Error


class DatabaseConnection:
    """
    A class to manage a connection to a MySQL database.

    Attributes:
        host (str): The hostname or IP address of the MySQL server.
        user (str): The username used to authenticate with the MySQL server.
        password (str): The password used to authenticate with the MySQL server.
        database (str): The name of the database to connect to. Defaults to 'db_algas'.
        connection (mysql.connector.connection.MySQLConnection): The active database connection.

    Methods:
        open_connection(): Establishes a connection to the MySQL database.
        close_connection(): Closes the active connection to the MySQL database, if it exists.
    """
    
    def __init__(self, user: str, password:str , database = 'db_algas', host = 'localhost'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None


    def open_connection(self):
        """
        Establishes a connection to the MySQL database.
        """        
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                print("Successfully connected to the MySQL database.")
        except Error as e:
            print(f"Error while connecting to MySQL: {e}")
            self.connection = None


    def close_connection(self):
        """
        Closes the active connection to the MySQL database, if it exists.
        """        
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection has been closed.")