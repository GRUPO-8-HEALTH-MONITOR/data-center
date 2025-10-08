# Nome do sensor: Dexcom G6
# Responsável: Igor de Morais

import os

from datetime import datetime, timedelta
import numpy as np, pandas as pd
import random
from services.connection_database import DatabaseConnection

def calcular_tempos(pontos_por_dia=288, intervalo_minutos=5):
    # define o inicio no multiplo de 5 minutos mais proximo
    inicio = datetime.now().replace(second=0, microsecond=0)
    inicio -= timedelta(minutes=inicio.minute % intervalo_minutos)
    # gera lista de tempos
    tempos = [inicio + timedelta(minutes=intervalo_minutos*i) for i in range(pontos_por_dia)]
    # converte para minutos desde meia-noite
    minutos = np.array([t.hour*60 + t.minute for t in tempos])
    return tempos, minutos

def curva_circadiana(minutos, config):
    # cria curva circadiana com pico ao meio-dia e vale a noite
    amplitude = config.get('amplitude_circadiana', 10)
    fase = 180  # pico as 12h (180 minutos apos 9h)
    return amplitude * np.sin(2 * np.pi * (minutos - fase) / 1440)

def pico_refeicao(minutos, hora_refeicao, config):
    # modela pico gaussiano apos refeicao
    hh, mm = map(int, hora_refeicao.split(':'))
    mu = hh*60 + mm + config.get('atraso_pico', 45)  # pico apos atraso
    sigma = config.get('sigma_pico', 30)  # largura do pico em minutos
    aumento_min, aumento_max = config.get('aumento_pico', (40, 90))
    amplitude = random.randint(aumento_min, aumento_max)
    return amplitude * np.exp(-0.5 * ((minutos - mu) / sigma) ** 2)

def fenomeno_amanhecer(minutos, config):
    # modela aumento suave entre 4-6h
    mu = config.get('hora_amanhecer', 5*60)  # pico as 5h
    sigma = config.get('sigma_amanhecer', 60)  # largura em minutos
    amplitude = config.get('amplitude_amanhecer', 20)
    return amplitude * np.exp(-0.5 * ((minutos - mu) / sigma) ** 2)

def gerar_dados_glicose(config):
    # configuracoes padrao
    glicose_base = config.get('glicose_base', 100)
    horarios_refeicoes = config.get('horarios_refeicoes', ['08:00', '13:00', '19:00'])
    glicose_min = config.get('glicose_min', 60)
    glicose_max = config.get('glicose_max', 250)
    nivel_ruido = config.get('nivel_ruido', 4)
    pontos_por_dia = 288  # 24h com medidas a cada 5 minutos

    # calcula tempos e minutos
    tempos, minutos = calcular_tempos(pontos_por_dia)

    # inicializa glicose com valor base como float
    glicose = np.full(pontos_por_dia, glicose_base, dtype=float)

    # adiciona curva circadiana
    glicose += curva_circadiana(minutos, config)

    # adiciona picos de refeicoes
    for hora in horarios_refeicoes:
        glicose += pico_refeicao(minutos, hora, config)

    # adiciona fenomeno do amanhecer
    glicose += fenomeno_amanhecer(minutos, config)

    # adiciona ruido aleatorio
    glicose += np.random.normal(0, nivel_ruido, pontos_por_dia)

    # limita valores entre minimo e maximo
    glicose = np.clip(glicose, glicose_min, glicose_max)

    # cria dataframe com resultados
    df = pd.DataFrame({
        'timestamp': [t.strftime('%Y-%m-%d %H:%M:%S') for t in tempos],
        'glicose': np.round(glicose, 1)
    })

    return df

def save_to_database(data_list, db_connection, fk_sensor=5, batch_size=50):
    # salva dados de glicose em lotes no banco de dados
    if not data_list:
        print("nenhum dado fornecido para salvar")
        return 0

    query = """
        INSERT INTO dados (valor, dt_hr_captura, fk_sensor)
        VALUES (%s, %s, %s)
    """

    # valida dados
    valid_data = []
    for data in data_list:
        try:
            glicose = float(data['glicose'])
            timestamp = data['timestamp']
            if not (isinstance(glicose, (int, float)) and 0 <= glicose <= 500):
                print(f"valor de glicose invalido: {glicose}")
                continue
            # valida timestamp (formato 'YYYY-MM-DD HH:MM:SS')
            datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            valid_data.append((glicose, timestamp, fk_sensor))
        except (ValueError, TypeError) as e:
            print(f"erro ao validar dado {data}: {e}")
            continue

    if not valid_data:
        print("nenhum dado valido para salvar")
        return 0

    try:
        cursor = db_connection.connection.cursor()
        total_saved = 0

        # processa em lotes
        for i in range(0, len(valid_data), batch_size):
            batch = valid_data[i:i + batch_size]
            cursor.executemany(query, batch)
            total_saved += len(batch)

        db_connection.connection.commit()
        return total_saved

    except Exception as e:
        print(f"erro ao salvar no banco de dados: {e}")
        db_connection.connection.rollback()
        return 0
    finally:
        cursor.close()


# Função para salvar dados em CSV
def salvar_dados_glicose_csv(dados_glicose, nome_arquivo=None, modo='w', separador=','):
    # salva dados de glicose em arquivo csv
    if dados_glicose.empty:
        print("nenhum dado fornecido para salvar")
        return False

    # define nome do arquivo com data atual se nao fornecido
    if nome_arquivo is None:
        data_atual = datetime.now().strftime('%Y%m%d_%H%M%S')
        nome_arquivo = f'dados_glicose_{data_atual}.csv'

    try:
        # cria dataframe
        if isinstance(dados_glicose, pd.DataFrame):
            df = dados_glicose.copy()
        else:
            df = pd.DataFrame(dados_glicose)

        # valida colunas
        if 'glicose' not in df.columns or 'timestamp' not in df.columns:
            print("dados devem conter colunas 'glicose' e 'timestamp'")
            return False

        # valida e converte timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        if df['timestamp'].isna().any():
            print("alguns timestamps sao invalidos")
            return False

        # valida glicose
        df['glicose'] = pd.to_numeric(df['glicose'], errors='coerce')
        if df['glicose'].isna().any() or (df['glicose'] < 0).any() or (df['glicose'] > 500).any():
            print("valores de glicose invalidos (devem estar entre 0 e 500)")
            return False

        # salva em csv
        df.to_csv(
            nome_arquivo,
            index=False,
            sep=separador,
            encoding='utf-8',
            date_format='%Y-%m-%d %H:%M:%S',
            mode=modo
        )
        print(f"salvos {len(df)} registros em '{nome_arquivo}'")
        return True

    except Exception as e:
        print(f"erro ao salvar csv: {e}")
        return False
    
if __name__ == "__main__":
    # config = load_config()

    config = {
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

    

    db_connection = DatabaseConnection(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    db_connection.open_connection()

    try:
       # gera dados
        glicose_df = gerar_dados_glicose(config)
        salvar_dados_glicose_csv(glicose_df)
        data_list = glicose_df.to_dict('records')
        save_to_database(data_list, db_connection, fk_sensor=1, batch_size=50)

    except KeyboardInterrupt:
        print("Cancela geração de dados.")
    
    finally:
        db_connection.close_connection()
