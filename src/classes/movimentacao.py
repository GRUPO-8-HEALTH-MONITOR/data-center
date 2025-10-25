from datetime import datetime, timedelta
import random


class Movimentacao:
    """
    Simula aceleração/giroscópio simples.
    start(infos_medica, duration_minutes, interval_seconds)
    """

    def __init__(self):
        pass

    def start(self, infos_medica: dict, duration_minutes=30, interval_seconds=1):
        now = datetime.now()
        steps = int((duration_minutes * 60) / interval_seconds)
        cen = infos_medica.get('cenario', 'padrao')
        records = []
        for i in range(steps):
            if cen == 'caminhada':
                accel = [round(random.uniform(-1,1),2) for _ in range(3)]
            elif cen == 'sedentario':
                accel = [round(random.uniform(-0.1,0.1),2) for _ in range(3)]
            else:
                accel = [round(random.uniform(-2,2),2) for _ in range(3)]

            ts = (now + timedelta(seconds=i * interval_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            records.append({'sensor':'movimentacao','aceleracao':{'x':accel[0],'y':accel[1],'z':accel[2]},'timestamp':ts})
        return records
