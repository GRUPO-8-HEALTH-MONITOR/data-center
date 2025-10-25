from datetime import datetime, timedelta
import random


class UmidadePele:
    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=60):
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        records = []
        for i in range(steps):
            if random.random() < 0.9:
                valor = round(random.uniform(30.0, 60.0),1)
            else:
                valor = round(random.uniform(10.0,90.0),1)
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor':'umidade_pele','valor':valor,'unidade':'%','timestamp':ts})
        return records
