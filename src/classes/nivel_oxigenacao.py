from datetime import datetime, timedelta
import random


class NivelOxigenacao:
    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=60):
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        records = []
        for i in range(steps):
            base = 97
            if infos_medica.get('condicao_clinica') == 'respiratorio':
                base = 92
            valor = max(80, min(100, round(random.gauss(base, 1.5),1)))
            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor':'nivel_oxigenacao','valor':valor,'unidade':'%','timestamp':ts})
        return records
