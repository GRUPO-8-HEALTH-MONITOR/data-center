# Nome do sensor: MPX5050DP

import os
import sys
import time
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))) # TODO: Refatorar isso para usar importações melhores

from Utils.py_utils import load_config
from services.connection_database import DatabaseConnection

def generate_pressure_data(config):
    """
    Generate random blood pressure data based on configuration.

    Args:
        config (dict): Configuration data with min and max pressure values.

    Returns:
        dict: Simulated blood pressure data.
    """
    min_pressure = config.get('min_pressure', 80)
    max_pressure = config.get('max_pressure', 120)

    systolic = random.randint(min_pressure, max_pressure)
    diastolic = random.randint(min_pressure // 2, max_pressure // 2)
    
    return {
        "systolic": systolic,
        "diastolic": diastolic,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }


def save_to_database(data, db_connection):
    """
    Save blood pressure data to the database.

    Args:
        data (dict): Blood pressure data to save.
        db_connection (DatabaseConnection): Database connection instance.
    """
    query = """
        INSERT INTO dados (valor, dt_hr_captura, fk_sensor)
        VALUES (%s, %s, %s)
    """
    formatted_pressure = f"{data['systolic']}/{data['diastolic']}"
    values = (formatted_pressure, data['timestamp'], 1)

    cursor = db_connection.connection.cursor()
    cursor.execute(query, values)

    db_connection.connection.commit()

    cursor.close()


if __name__ == "__main__":
    config = load_config()

    db_connection = DatabaseConnection(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    db_connection.open_connection()

    try:
        batch_size = 100
        iterations = 10

        for _ in range(iterations):
            batch_data = []

            for _ in range(batch_size):
                pressure_data = generate_pressure_data(config)
                
                print(pressure_data)

                batch_data.append(pressure_data)

            for data in batch_data:
                save_to_database(data, db_connection)

    except KeyboardInterrupt:
        print("Stopping data generation.")
    
    finally:
        db_connection.close_connection()
