import pandas as pd
import os
import boto3
import mysql.connector
import json
from datetime import datetime
from dotenv import load_dotenv

BKT_RAW = "health-data-atv-raw"
load_dotenv()
key = json.loads(os.getenv('AWS_SECRET'))
class DataIngestion:
    def __init__(self):
        print(key["host"])
        self.idade = 0
        self.peso = 0.0
        self.condicao_clinica = ''
        self.config_glicose = {
            'glicose_base': 100,
            'amplitude_circadiana': 12,
            'horarios_refeicoes': ['07:30', '12:30', '18:30'],
            'aumento_pico': (50, 100),
            'atraso_pico': 45,
            'sigma_pico': 35,
            'hora_amanhecer': 5*60,
            'amplitude_amanhecer': 25,
            'sigma_amanhecer': 70,
            'glicose_min': 60,
            'glicose_max': 250,
            'nivel_ruido': 5
        }  
        self.config_pressao = {
            'min_pressure': 90,
            'max_pressure': 120,
        }
        
    def load_data(self):
        return
    
if __name__ == "__main__":
    ingestion = DataIngestion()
        
