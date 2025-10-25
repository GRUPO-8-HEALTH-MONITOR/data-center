# TRATATIVA PARA RODAR EM DRY-RUN SEM MYSQL CONNECTOR
try:
    import mysql.connector
    from mysql.connector import Error
    MYSQL_AVAILABLE = True
except Exception:
    mysql = None
    Error = Exception
    MYSQL_AVAILABLE = False


class DatabaseConnection: 
    def __init__(self, user: str, password:str , database = 'db_algas', host = 'localhost'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None


    def open_connection(self):   
        if not MYSQL_AVAILABLE:
            print("mysql connector not installed; database operations will be disabled (dry-run).")
            self.connection = None
            return

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
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection has been closed.")