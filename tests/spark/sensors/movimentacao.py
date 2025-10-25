# Nome do sensor: MPU6050
# Responsável: Felipe Santos

import csv
import time
import random
# from services.azure_service import AzureIoTHub

class MPU6050:
    def __init__(self):
        # Inicializa o sensor (simulado)
        print("Sensor MPU6050 inicializado.")
        self.cenario = "padrao"  # Cenário padrão

    def configurar_cenario(self, cenario):
        """
        Configura o cenário para a geração de dados.
        Cenários disponíveis: 'caminhada', 'sedentario', 'padrao'.
        """
        if cenario in ["caminhada", "sedentario", "padrao"]:
            self.cenario = cenario
            print(f"Cenário configurado para: {cenario}")
        else:
            raise ValueError("Cenário inválido. Escolha entre 'caminhada', 'sedentario' ou 'padrao'.")

    def gerar_dados(self):
        """
        Gera dados simulados para o sensor MPU6050 com base no cenário configurado.
        Retorna um dicionário com valores de aceleração e giroscópio.
        """
        if self.cenario == "caminhada":
            dados = {
                "aceleracao": {
                    "x": round(random.uniform(-1, 1), 2),
                    "y": round(random.uniform(-1, 1), 2),
                    "z": round(random.uniform(0.8, 1.2), 2),
                },
                "giroscopio": {
                    "x": round(random.uniform(-50, 50), 2),
                    "y": round(random.uniform(-50, 50), 2),
                    "z": round(random.uniform(-50, 50), 2),
                },
            }
        elif self.cenario == "sedentario":
            dados = {
                "aceleracao": {
                    "x": round(random.uniform(-0.1, 0.1), 2),
                    "y": round(random.uniform(-0.1, 0.1), 2),
                    "z": round(random.uniform(0.9, 1.1), 2),
                },
                "giroscopio": {
                    "x": round(random.uniform(-5, 5), 2),
                    "y": round(random.uniform(-5, 5), 2),
                    "z": round(random.uniform(-5, 5), 2),
                },
            }
        else:
            # Cenário padrão
            dados = {
                "aceleracao": {
                    "x": round(random.uniform(-2, 2), 2),
                    "y": round(random.uniform(-2, 2), 2),
                    "z": round(random.uniform(-2, 2), 2),
                },
                "giroscopio": {
                    "x": round(random.uniform(-250, 250), 2),
                    "y": round(random.uniform(-250, 250), 2),
                    "z": round(random.uniform(-250, 250), 2),
                },
            }
        return dados

    def interpretar_dados(self, dados):
        """
        Interpreta os dados do sensor para determinar o estado do usuário.
        Retorna uma string com a interpretação.
        """
        aceleracao = dados["aceleracao"]
        giroscopio = dados["giroscopio"]

        # Magnitude da aceleração (em g)
        magnitude_aceleracao = (aceleracao["x"]**2 + aceleracao["y"]**2 + aceleracao["z"]**2)**0.5

        if magnitude_aceleracao < 0.5 and all(abs(giroscopio[axis]) < 10 for axis in ["x", "y", "z"]):
            return "Usuário está parado"
        elif magnitude_aceleracao > 2.5:
            return "Possível queda detectada"
        elif magnitude_aceleracao >= 0.5 and magnitude_aceleracao <= 2.5:
            return "Usuário está em movimento"
        else:
            return "Estado desconhecido"

    def iniciar_monitoramento(self, intervalo=1, dados_csv="dados_sensor.csv", performance_csv="performance.csv"):
        """
        Inicia o monitoramento contínuo do sensor, gerando dados a cada intervalo de tempo,
        interpretando os dados e salvando em arquivos CSV.
        """
        with open(dados_csv, mode="w", newline="") as dados_file, open(performance_csv, mode="w", newline="") as perf_file:
            dados_writer = csv.writer(dados_file)
            perf_writer = csv.writer(perf_file)

            dados_writer.writerow(["timestamp", "acel_x", "acel_y", "acel_z", "giro_x", "giro_y", "giro_z", "interpretacao"])
            perf_writer.writerow(["timestamp", "tempo_execucao"])

            try:
                while True:
                    inicio = time.time()  # Marca o início da execução

                    dados = self.gerar_dados()
                    interpretacao = self.interpretar_dados(dados)

                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    dados_writer.writerow([
                        timestamp,
                        dados["aceleracao"]["x"], dados["aceleracao"]["y"], dados["aceleracao"]["z"],
                        dados["giroscopio"]["x"], dados["giroscopio"]["y"], dados["giroscopio"]["z"],
                        interpretacao
                    ])

                    tempo_execucao = time.time() - inicio
                    perf_writer.writerow([timestamp, round(tempo_execucao, 4)])

                    print("Dado Bruto:", dados)
                    print("Interpretação:", interpretacao)

                    time.sleep(intervalo)
            except KeyboardInterrupt:
                print("Monitoramento interrompido pelo usuário.")

    def salvar_dados_caminhada(self, arquivo_csv="dados_caminhada.csv", intervalo=1):
        """
        Gera e salva dados simulados continuamente para o cenário de caminhada em um arquivo CSV.
        """
        self.cenario = "caminhada"
        self._salvar_dados_cenario_continuo(arquivo_csv, intervalo)

    def salvar_dados_sedentario(self, arquivo_csv="dados_sedentario.csv", intervalo=1):
        """
        Gera e salva dados simulados continuamente para o cenário sedentário em um arquivo CSV.
        """
        self.cenario = "sedentario"
        self._salvar_dados_cenario_continuo(arquivo_csv, intervalo)

    def _salvar_dados_cenario_continuo(self, arquivo_csv, intervalo):
        """
        Método interno para salvar dados simulados continuamente de um cenário específico em um arquivo CSV.
        """
        with open(arquivo_csv, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "acel_x", "acel_y", "acel_z", "giro_x", "giro_y", "giro_z", "interpretacao"])

            try:
                while True:
                    dados = self.gerar_dados()
                    interpretacao = self.interpretar_dados(dados)
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

                    writer.writerow([
                        timestamp,
                        dados["aceleracao"]["x"], dados["aceleracao"]["y"], dados["aceleracao"]["z"],
                        dados["giroscopio"]["x"], dados["giroscopio"]["y"], dados["giroscopio"]["z"],
                        interpretacao
                    ])

                    print(f"[{timestamp}] Dado Bruto: {dados}, Interpretação: {interpretacao}")
                    time.sleep(intervalo)
            except KeyboardInterrupt:
                print(f"Monitoramento do cenário '{self.cenario}' interrompido pelo usuário.")

    def enviar_dados_para_azure(self, intervalo=1):
        """
        Gera dados simulados continuamente e envia para o Azure IoT Hub.
        """
        # azure_client = AzureIoTHub()
        try:
            while True:
                dados = self.gerar_dados()
                interpretacao = self.interpretar_dados(dados)
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

                # Criar mensagem para envio
                mensagem = {
                    "timestamp": timestamp,
                    "aceleracao": dados["aceleracao"],
                    "giroscopio": dados["giroscopio"],
                    "interpretacao": interpretacao
                }

                # Enviar mensagem para o Azure IoT Hub
                # azure_client.send_message(str(mensagem))
                print(f"Dados enviados para o Azure IoT Hub: {mensagem}")

                time.sleep(intervalo)
        except KeyboardInterrupt:
            print("Envio de dados para o Azure IoT Hub interrompido pelo usuário.")

if __name__ == "__main__":
    sensor = MPU6050()
    try:
        sensor.enviar_dados_para_azure(intervalo=0.2)
    except KeyboardInterrupt:
        print("Encerrando envio de dados para o Azure IoT Hub.")