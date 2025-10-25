from datetime import datetime, timedelta
import random


class TemperaturaCorporal:
    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=60):
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        records = []
        for i in range(steps):
            if random.random() < 0.9:
                valor = round(random.uniform(36.0, 37.5),1)
            else:
                valor = round(random.uniform(38.0, 40.0),1)
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor':'temperatura_corporal','valor':valor,'unidade':'C','timestamp':ts})
        return records
