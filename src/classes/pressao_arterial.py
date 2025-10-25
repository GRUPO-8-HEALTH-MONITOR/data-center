from datetime import datetime, timedelta
import random


class PressaoArterial:
    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=60):
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        records = []
        for i in range(steps):
            syst = random.randint(100, 140)
            dias = random.randint(60, 90)
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor':'pressao_arterial','valor':f"{syst}/{dias}",'unidade':'mmHg','timestamp':ts})
        return records
