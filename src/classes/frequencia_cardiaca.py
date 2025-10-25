from datetime import datetime, timedelta
import math
import random


class FreqCardiaca:
    """
    Simula frequência cardíaca.

    start(infos_medica, duration_minutes=30, interval_seconds=60) -> list[dict]
    """

    def __init__(self):
        self.variacao_max = 5

    def gerar_valor(self, bpm_base, t):
        oscilacao = self.variacao_max * math.sin(2 * math.pi * t / 60)
        ruido = random.uniform(-1.5, 1.5)
        return round(bpm_base + oscilacao + ruido, 2)

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=60):
        idade = infos_medica.get('idade', 45)
        if idade > 65:
            bpm_base = random.randint(50, 60)
        elif 18 <= idade <= 65:
            bpm_base = random.randint(70, 78)
        elif 2 < idade < 18:
            bpm_base = random.randint(80, 100)
        else:
            bpm_base = random.randint(120, 140)

        records = []
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        for i in range(steps):
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({
                'sensor': 'frequencia_cardiaca',
                'valor': self.gerar_valor(bpm_base, i),
                'unidade': 'bpm',
                'timestamp': ts
            })

        return records
