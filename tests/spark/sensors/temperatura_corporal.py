# Nome do sensor: MLX90614
# Responsável: Rafael Scheneider
import sys
import os
import random
import pandas as pd
from datetime import datetime
from mysql.connector import Error

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from services.connection_database import DatabaseConnection


def bulk_insert(connection, dataframe, sensor_id):
    if dataframe.empty or not connection:
        return

    cursor = connection.cursor()
    
    try:
        query = """
            INSERT INTO dados (valor, dt_hr_captura, fk_sensor)
            VALUES (%s, %s, %s)
        """
        
        data_to_insert = [(row['valor'], row['dt_hr_captura'], sensor_id) for _, row in dataframe.iterrows()]
        cursor.executemany(query, data_to_insert)
        connection.commit()
        
        print(f"Inserted batch of {len(data_to_insert)} temperature records.")
        
        
    except Error as e:
        print(f"Failed to insert batch: {e}")
        
    finally:
        cursor.close()
        
        
        

def simulate_temperature():
    db = DatabaseConnection(user='', password='')
    db.open_connection()
    sensor_id = 1

    df = pd.DataFrame(columns=['valor', 'dt_hr_captura'])

    try:
        df = generate_data()


        print("Temperature simulation stopped by user.")
        
        if not df.empty:
            bulk_insert(db.connection, df, sensor_id)
    finally:
        db.close_connection()
        
        
        
def generate_data():
    if random.random() < 0.8:
        value = round(random.uniform(36.0, 37.5), 1)
        
    else:
        value = round(random.uniform(38.0, 40.0), 1)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = pd.DataFrame({'valor': [value], 'dt_hr_captura': [timestamp]})

    return df

if __name__ == "__main__":
    simulate_temperature()