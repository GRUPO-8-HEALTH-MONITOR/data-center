# Nome do sensor: MAX30102
# Responsável: Isabela Noronha
import random
import time
import csv
import matplotlib.pyplot as plt
import numpy as np
from colorama import Fore, init
# from frequencia_cardiaca import FreqCardiaca

class NivelOxigenacao:
    def __init__(self, idade_paciente=45, peso=70, condicao_clinica="normal"):
        self.idade_paciente = idade_paciente
        self.peso = peso
        self.condicao_clinica = condicao_clinica
        init(autoreset=True)  # Inicializa o colorama para resetar cores automaticamente
        # self.freq = FreqCardiaca(idade_paciente=idade_paciente)
        
    def gerar_valores_captura(self, tempo, peso, idade, condicao_clinica):
        pulse_freq = random.uniform(0.8, 1.5)
        
        ajuste_sinal = 1.0
        if peso > 100:
            ajuste_sinal *= 0.9  # Reduz amplitude para simular menor eficiência respiratória
        if condicao_clinica == "cardiaco":
            ajuste_sinal *= 0.85
        elif condicao_clinica == "respiratorio":
            ajuste_sinal *= 0.8
            
        red_ac = (0.50 * ajuste_sinal) + 0.05 * np.sin(2 * np.pi * pulse_freq * tempo) + random.gauss(0, 0.005)
        ir_ac = (0.80 * ajuste_sinal) + 0.08 * np.sin(2 * np.pi * pulse_freq * tempo) + random.gauss(0, 0.005)
        red_dc = 1.00 + random.gauss(0, 0.01)
        ir_dc = 1.20 + random.gauss(0, 0.015)
        return red_ac, red_dc, ir_ac, ir_dc

    def calcula_oxigenacao(self, red_ac, red_dc, ir_ac, ir_dc):
        razao = (red_ac / red_dc) / (ir_ac / ir_dc)
        spo = 104 - 17 * razao
        return max(min(spo, 100), 0)

    def valida_oxigenacao(spo):
        if spo < 90:
            print(Fore.RED + "Urgente! Oxigenação baixa.")
            return "Urgente! Oxigenação baixa."
        elif 90 <= spo <= 95:
            print(Fore.YELLOW + "Oxigenação precária.")
            return "Oxigenação precária."
        elif 95 < spo <= 100:
            print(Fore.GREEN + "Oxigenação normal.")
            return "Oxigenação normal."
        else:
            print(Fore.MAGENTA + "Valide o código. Algo está errado.")
            return "Valide o código. Algo está errado."

    def captura_oxigenacao(self):
        print("Iniciando captura de dados... (Ctrl+C para parar)\n")

        tempo = []
        spo_list = []

        plt.ion()
        fig, ax = plt.subplots()

        start_time = time.time()

        while True:
            tempo_atual = (time.time() - start_time)/60
            red_ac, red_dc, ir_ac, ir_dc = self.gerar_valores_captura(tempo_atual)

            spo = self.calcula_oxigenacao(red_ac, red_dc, ir_ac, ir_dc)

            tempo.append(tempo_atual)
            spo_list.append(spo)

            print(f"Valores capturados:")
            print(f"  RED_AC: {red_ac:.4f} | RED_DC: {red_dc:.4f}")
            print(f"  IR_AC : {ir_ac:.4f} | IR_DC : {ir_dc:.4f}")
            print(f"Oxigenação calculada: {spo:.2f}%")
            self.valida_oxigenacao(spo)

            ax.clear()
            ax.plot(tempo, spo_list, label="SpO₂", color="green")
            ax.axhline(y=90, color="red", linestyle="--", label="Limite mínimo (90%)")
            ax.axhline(y=100, color="blue", linestyle="--", label="Limite máximo (100%)")
            ax.set_xlabel("Tempo (min)")
            ax.set_ylabel("Oxigenação (%)")
            ax.set_title("Nível de Oxigenação ao longo do tempo")
            ax.set_ylim(80, 105)
            ax.legend()
            ax.grid(True)
            plt.tight_layout()
            plt.pause(0.01)

            print("-" * 50)
            time.sleep(2)

    def captura_sinais(self):
        print("Iniciando captura de sinais RED_AC e IR_AC... (Ctrl+C para parar)\n")

        tempo = []
        red_ac_list = []
        ir_ac_list = []

        plt.ion()
        fig, ax = plt.subplots()

        start_time = time.time()

        try:
            while True:
                tempo_atual = (time.time() - start_time) / 60  # Converter tempo para minutos
                red_ac, red_dc, ir_ac, ir_dc = self.gerar_valores_captura(tempo_atual * 60)  # Converter de volta para segundos

                tempo.append(tempo_atual)
                red_ac_list.append(red_ac)
                ir_ac_list.append(ir_ac)

                print(f"Valores capturados:")
                print(f"  RED_AC: {red_ac:.4f}")
                print(f"  IR_AC : {ir_ac:.4f}")
                print(f"  Tempo decorrido: {tempo_atual:.2f} minutos")

                # Atualizar o gráfico
                ax.clear()
                ax.plot(tempo, red_ac_list, label="RED_AC", color="red")
                ax.plot(tempo, ir_ac_list, label="IR_AC", color="blue")
                ax.set_xlabel("Tempo (minutos)")
                ax.set_ylabel("Amplitude")
                ax.set_title("Sinais RED_AC e IR_AC ao longo do tempo")
                ax.legend()
                ax.grid(True)
                plt.tight_layout()
                plt.pause(0.01)

                print("-" * 50)
                time.sleep(2)

        except KeyboardInterrupt:
            print(Fore.CYAN + "\nMonitoramento encerrado pelo usuário. Até logo!")
            plt.close()

    def main(self):
        print("Escolha uma opção:")
        print("1 - Visualizar sinais RED_AC e IR_AC")
        print("2 - Visualizar oxigenação (SpO₂)")

        escolha = input("Digite 1 ou 2: ")

        try:
            if escolha == "1":
                self.captura_sinais()  # Função que plota RED_AC e IR_AC
            elif escolha == "2":
                self.captura_oxigenacao()  # Função que plota SpO₂
            else:
                print("Opção inválida.")
        except KeyboardInterrupt:
            print(Fore.CYAN + "\nMonitoramento encerrado pelo usuário. Até logo!")
            plt.close()



# if __name__ == "__main__":
#     oxi = NivelOxigenacao()
#     oxi.main()
