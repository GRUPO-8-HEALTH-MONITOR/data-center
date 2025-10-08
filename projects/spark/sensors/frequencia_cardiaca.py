# Nome do sensor: MAX30102
# Responsável: Larissa Sonoda
# import main as fn
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import main as f
from utils.py_utils import get_configs 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))) # TODO: Refatorar isso para usar importações melhores
import random
import math
import csv
import logging
from datetime import datetime 
from services.connection_database import DatabaseConnection
from time import sleep
config = get_configs()
sensors = config.get('sensors', []) # type: ignore
global logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
class FreqCardiaca:
    def __init__(self, db_connection: DatabaseConnection, idade_paciente=45):
        # self.batch = batch
        self.min_bpm = next((sensor['value_min'] for sensor in sensors if sensor['name'] == 'MAX30102'), None)
        self.variacao_max = 5
        self.idade_paciente = idade_paciente
        self.max_bpm = 220 - self.idade_paciente
        self.fk_sensor = None
        self.t = 0
        # db_connection.open_connection()
        # self.db_connection = db_connection
        # self.cursor = db_connection.connection.cursor()
        self.dados = []
        if idade_paciente > 65:
            self.bpm_base = random.randint(50, 60)
        elif idade_paciente >= 18 and idade_paciente <= 65:
            self.bpm_base = random.randint(70, 78)
        elif idade_paciente > 2 and idade_paciente < 18:
            self.bpm_base = random.randint(80, 100)
        else:
            self.bpm_base = random.randint(120, 140)
        
    def gerar_dados(self):
         # Oscilação senoidal suave + ruído randômico leve
        oscilacao = self.variacao_max * math.sin(2 * math.pi * self.t / 60)
        ruido = random.uniform(-1.5, 1.5)
        bpm = self.bpm_base + oscilacao + ruido
        self.t += 1
        # Garantir que o valor está dentro dos limites do sensor
        return {'bpm':round(max(self.min_bpm, min(self.max_bpm, bpm)), 2), 'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    
    def gerar_batch(self):
        sensor_id = "SELECT id FROM sensor s WHERE s.nome = %s"
        self.cursor.execute(sensor_id, ('MAX30102',))
        self.fk_sensor = self.cursor.fetchone()[0]
        for _ in range(self.batch):
            bpm = self.gerar_dados()
            dado = {
                "valor": bpm,
                "dt_hr_captura": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "fk_sensor": self.fk_sensor
            }
            self.dados.append(dado)
        return self.dados
    
    def salvar_em_csv(self,dados, nome_arquivo='dados_bpm.csv'):
        if not dados:
            print("Nenhum dado para salvar.")
            return

        colunas = dados[0].keys()
        arq_existe = os.path.exists(nome_arquivo)
        header = not arq_existe or os.star(nome_arquivo).st_size == 0
        with open(nome_arquivo, mode='a', newline='') as arquivo_csv:
            writer = csv.DictWriter(arquivo_csv, fieldnames=colunas)
            if header:
                writer.writeheader()
                
            writer.writerows(dados)

        print(f"Arquivo CSV '{nome_arquivo}' salvo com sucesso!")
        
    def salvar_banco(self, dados=None):
        if dados:
            self.dados = dados
        if not self.fk_sensor:
            logger.error('Sensor não encontrado!')
        else:
            valores = [(d['valor'], d['dt_hr_captura'], d['fk_sensor']) for d in self.dados]
            sql = "INSERT INTO dados (valor, dt_hr_captura, fk_sensor) VALUES (%s, %s, %s)"
            self.cursor.executemany(sql, valores)

            self.db_connection.connection.commit()
            # logger.debug(cursor.rowcount, "record inserted.")
    
if __name__ == "__main__":    
      
    db_connection = DatabaseConnection(
        user=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD')
    )
    db_connection.open_connection()
    
    try:
        logger.info(db_connection)
        freq = FreqCardiaca(db_connection)
        while True:
            dados = []
            for _ in range(60):
                dados.append(freq.gerar_dados())
                print(f"Frequência cardíaca gerada: {dados[-1]} BPM")
                sleep(60)
            freq.salvar_em_csv(dados, nome_arquivo='dados_bpm.csv')
            # freq.salvar_banco(dados)
            
            
        # idade = int(input('Informe a idade do paciente que está sendo monitorado para melhor análise: ')) 
        # batch_size = int(input('Quantidade de dados para o batch: '))   
        # freq_cardiaca = FreqCardiaca(idade, batch_size, db_connection)
        # freq_cardiaca.gerar_batch()
        # freq_cardiaca.salvar_banco()
        #utilizar a fim de receber insights ou futuramente para as análises no Google Sheets
        # freq_cardiaca.salvar_em_csv(values) 
        
        

    except KeyboardInterrupt:
        freq.salvar_em_csv(dados, nome_arquivo='dados_bpm.csv')
        print("Stopping data generation.")
    
    finally:
        db_connection.close_connection()
    