from datetime import datetime, timedelta
import random
import math


class Glicose:
    """
    Simula série temporal de glicose. start(infos_medica, duration_minutes, interval_seconds)
    retorna lista de dicts com 'glicose' e 'timestamp'.
    """

    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=300):
        # intervalo default 5 minutos (300s)
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        base = infos_medica.get('glicose_base', 100)
        records = []
        for i in range(steps):
            minutos = (i * interval_seconds) / 60.0
            circ = 8 * math.sin(2 * math.pi * minutos / 1440.0)
            ruido = random.gauss(0, 3)
            valor = max(40.0, min(400.0, base + circ + ruido))
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor': 'glicose', 'valor': round(valor,1), 'unidade':'mg/dL', 'timestamp': ts})
        return records
