import os

from sensors.movimentacao import MPU6050
from sensors.glicose import gerar_dados_glicose
from sensors.frequencia_cardiaca import FreqCardiaca
from sensors.nivel_oxigenacao import NivelOxigenacao
from sensors.temperatura_corporal import generate_data
from sensors.pressao_arterial import generate_pressure_data
from sensors.umidade_pele import generate_data as generate_umi_pele_data

import json
import time
from datetime import datetime

class DataIngestion:
    def __init__(self):
        self.idade_paciente = 45
        self.peso = 70
        self.condicao_clinica = "normal"
        
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
        
        # self.freq_cardiaca = FreqCardiaca(None, idade_paciente=self.idade_paciente)
        # # Create a mock database connection or pass required parameters
        # # Assuming FreqCardiaca can work without db_connection for simulation
        # try:
        #     self.freq_cardiaca = FreqCardiaca(db_connection=None, idade_paciente=self.idade_paciente)
        # except TypeError:
        #     # If FreqCardiaca requires a specific db_connection type, create a mock one
        #     class MockDatabaseConnection:
        #         pass
        #     self.freq_cardiaca = FreqCardiaca(MockDatabaseConnection(), idade_paciente=self.idade_paciente)
        self.movimentacao = MPU6050()
        self.dados_coletados = []

    def coletar_dados_sensores(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        bpm_data = 0
        glicose = gerar_dados_glicose(self.config_glicose)
        temp_corporal = generate_data()
        umi_pele = generate_umi_pele_data()
        pressao_arterial = generate_pressure_data(self.config_pressao)
        
        mpu6050 = self.movimentacao.gerar_dados()
        aceleracao = mpu6050["aceleracao"]
        giroscopio = mpu6050["giroscopio"]
        magnitude_aceleracao = (aceleracao["x"]**2 + aceleracao["y"]**2 + aceleracao["z"]**2)**0.5
        
        # red_ac, red_dc, ir_ac, ir_dc = self.oxigenacao.gerar_valores_captura(
        #     time.time(), self.peso, self.idade_paciente, self.condicao_clinica
        # )
        #spo = self.oxigenacao.calcula_oxigenacao(red_ac, red_dc, ir_ac, ir_dc)
        
        dados = {
            "timestamp": timestamp,
            #"bpm": bpm_data["bpm"],
            #"spo2": round(spo, 2),
            "glicose": glicose['glucose'],
            "pressao_sistolica": pressao_arterial['systolic'],
            "pressao_diastolica": pressao_arterial['diastolic'],
            "aceleracao_x": aceleracao["x"],
            "aceleracao_y": aceleracao["y"],
            "aceleracao_z": aceleracao["z"],
            "giroscopio_x": giroscopio["x"],
            "giroscopio_y": giroscopio["y"],
            "giroscopio_z": giroscopio["z"],
            "magnitude_aceleracao": magnitude_aceleracao,
            "temperatura": temp_corporal['valor'].values[0],
            "umidade_pele": umi_pele['valor'].values[0]
        }
        
        return dados

    def salvar_dados_raw(self, dados_batch, output_dir="../../output"):
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_health_data_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(dados_batch, f, indent=2)
        
        print(f"Dados salvos em: {filepath}")
        return filepath

    def executar_coleta(self, duration_minutes=5, interval_seconds=2):
        print(f"Iniciando coleta de dados por {duration_minutes} minutos...")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        batch_dados = []
        
        try:
            while time.time() < end_time:
                dados = self.coletar_dados_sensores()
                batch_dados.append(dados)
                
                print(f"TIME: {dados['timestamp']} | BPM: {dados['bpm']}bpm | SpO₂: {dados['spo2']}% | Glicose: {dados['glicose']} mg/dL | Pressão: {dados['pressao_sistolica']}/{dados['pressao_diastolica']} mmHg | Temperatura: {dados['temperatura']}°C")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\nColeta interrompida pelo usuário...")
        
        filepath = self.salvar_dados_raw(batch_dados)
        print(f"Coleta finalizada. {len(batch_dados)} registros coletados.")
        return filepath

if __name__ == "__main__":
    ingestion = DataIngestion()
    ingestion.executar_coleta()
