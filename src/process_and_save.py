"""
Consumidor simples para processar os arquivos gerados

Le arquivos JSON, faz a limpeza mínima com "Spark", salva em trusted e
insere no banco de dados (tabela registro) quando houver mapeamento paciente_sensor.
"""
import os
import json
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from services.connection_database import DatabaseConnection

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] > %(name)s: %(message)s')
LOGGER.setLevel(logging.INFO)


def list_raw_files(raw_dir):
    if not os.path.isdir(raw_dir):
        return []
    files = []
    now = time.time()
    for f in os.listdir(raw_dir):
        if not f.endswith('.json'):
            continue
        p = os.path.join(raw_dir, f)
        try:
            st = os.stat(p)
        except Exception:
            continue

        if st.st_size < 200:
            continue
        if now - st.st_mtime < 1.0:
            # arquivo muito recente, pular (provavelmente em escrita)
            LOGGER.debug('Pulando arquivo recente: %s (age=%.2fs)', p, now - st.st_mtime)
            continue
        files.append(p)
    LOGGER.info('Encontrados %d arquivos raw válidos em %s', len(files), raw_dir)
    return sorted(files)


def process_file(path, db: DatabaseConnection, trusted_dir):
    LOGGER.info('Iniciando processamento de arquivo: %s', path)
    try:
        st = os.stat(path)
        LOGGER.debug('Tamanho do arquivo: %d bytes, mtime: %s', st.st_size, time.ctime(st.st_mtime))
    except Exception:
        LOGGER.debug('Não foi possível obter estatísticas do arquivo: %s', path)

    payload = None
    last_err = None
    for attempt in range(3):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                payload = json.load(f)
            break
        except json.JSONDecodeError as e:
            last_err = e
            LOGGER.warning('JSONDecodeError ao ler %s (attempt %d): %s', path, attempt + 1, e)
            time.sleep(0.5 * (attempt + 1))
            continue
        except Exception as e:
            last_err = e
            LOGGER.exception('Erro ao abrir %s: %s', path, e)
            break

    if payload is None:
        # Pasta 'broken' para investigação
        bad_dir = os.path.join(os.path.dirname(path), 'broken')
        os.makedirs(bad_dir, exist_ok=True)
        bad_path = os.path.join(bad_dir, os.path.basename(path) + '.bad')
        try:
            os.replace(path, bad_path)
        except Exception:
            LOGGER.exception('Falha ao mover arquivo corrompido %s', path)
        LOGGER.error('Arquivo JSON inválido movido para %s: %s', bad_path, last_err)
        return

    try:
        paciente = payload.get('paciente', {})
        records = payload.get('records', [])
    except Exception as e:
        LOGGER.exception('Payload inesperado no arquivo %s: %s', path, e)
        return

    LOGGER.info('Arquivo %s: paciente_id=%s nome=%s registros=%d', path, paciente.get('id'), paciente.get('nome'), len(records))

    if not records:
        LOGGER.info('Arquivo %s sem registros — removendo', path)
        os.remove(path)
        return

    df = pd.json_normalize(records)
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])
    
    records_proc = json.loads(df.to_json(orient='records', date_format='iso'))

    os.makedirs(trusted_dir, exist_ok=True)
    base = os.path.basename(path)
    trusted_path = os.path.join(trusted_dir, base)
    with open(trusted_path, 'w', encoding='utf-8') as f:
        json.dump(records_proc, f, ensure_ascii=False, indent=2)

    LOGGER.info('Arquivo processado e salvo em trusted: %s (registros: %d)', trusted_path, len(records_proc))

    if db and getattr(db, 'connection', None):
        cursor = db.connection.cursor(dictionary=True)
        insert_q = 'INSERT INTO registro (valor, created_at, paciente_sensor_id) VALUES (%s, %s, %s)'

        try:
            cursor.execute('SELECT id, nome FROM sensor')
            sensor_rows = cursor.fetchall() or []

        except Exception:
            sensor_rows = []
        def canon(s):
            return ''.join(ch for ch in str(s or '').lower() if ch.isalnum())

        sensors_map = {canon(r.get('nome')): r.get('id') for r in sensor_rows}

        try:
            cursor.execute('SELECT id, sensor_id FROM paciente_sensor WHERE paciente_id=%s', (paciente.get('id'),))
            ps_rows = cursor.fetchall() or []
        except Exception:
            ps_rows = []
        paciente_sensor_map = {r.get('sensor_id'): r.get('id') for r in ps_rows}

        inserted = 0
        skipped_no_sensor = 0
        skipped_no_paciente_sensor = 0
        skipped_invalid_value = 0
        total_rows = 0
        altered_id_autoinc = False

        for _, row in df.iterrows():
            total_rows += 1
            sensor = row.get('sensor') or row.get('sensor_name') or row.get('nome')
            valor = row.get('valor') if 'valor' in row.index else None
            ts = row.get('timestamp')

            if pd.isna(sensor):
                LOGGER.debug('Linha sem sensor definido — pulando')
                skipped_no_sensor += 1
                continue

            key = canon(sensor)
            sensor_id = sensors_map.get(key)

            # fallback
            if sensor_id is None:
                try:
                    cursor.execute('SELECT id, nome FROM sensor WHERE nome LIKE %s LIMIT 1', (f'%{sensor}%',))
                    res = cursor.fetchone()
                    if res:
                        sensor_id = res.get('id')
                        sensors_map[canon(res.get('nome'))] = sensor_id
                except Exception:
                    LOGGER.exception('Erro consultando sensor %s', sensor)

            if sensor_id is None:
                skipped_no_sensor += 1
                LOGGER.debug('Sensor não encontrado para %s', sensor)
                continue

            paciente_sensor_id = paciente_sensor_map.get(sensor_id)
            if paciente_sensor_id is None:
                skipped_no_paciente_sensor += 1
                LOGGER.debug('Nenhum mapeamento paciente_sensor para paciente=%s sensor_id=%s', paciente.get('id'), sensor_id)
                continue

            if valor is None or pd.isna(valor):
                skipped_invalid_value += 1
                LOGGER.debug('Valor inválido para sensor %s: %s', sensor, valor)
                continue

            # Format timestamp
            if hasattr(ts, 'strftime'):
                ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
            else:
                ts_str = str(ts)

            try:
                cursor.execute(insert_q, (str(valor), ts_str, paciente_sensor_id))
                inserted += 1
            except Exception as e:
                # Correção de IA
                # Detectar erro típico quando a coluna id não tem AUTO_INCREMENT no schema
                msg = str(e)
                if ("doesn't have a default value" in msg or '1364' in msg) and not altered_id_autoinc:
                    LOGGER.warning('Erro de schema detectado ao inserir: %s. Tentando adicionar AUTO_INCREMENT em registro.id', msg)
                    try:
                        cursor.execute('ALTER TABLE registro MODIFY COLUMN id INT NOT NULL AUTO_INCREMENT')
                        db.connection.commit()
                        altered_id_autoinc = True
                        cursor.execute(insert_q, (str(valor), ts_str, paciente_sensor_id))
                        inserted += 1
                        continue
                    except Exception:
                        LOGGER.exception('Falha ao aplicar ALTER TABLE para registro.id')
                        db.connection.rollback()
                LOGGER.exception('Erro ao inserir registro: sensor=%s paciente_sensor_id=%s (%s)', sensor, paciente_sensor_id, msg)
                db.connection.rollback()

        try:
            db.connection.commit()
        except Exception:
            LOGGER.exception('Erro no commit final')
        try:
            cursor.close()
        except Exception:
            pass

        LOGGER.info('Linhas no arquivo: %d; Inseridos %d registros no banco (skipped: no_sensor=%d, no_paciente_sensor=%d, invalid_value=%d)',
                    total_rows, inserted, skipped_no_sensor, skipped_no_paciente_sensor, skipped_invalid_value)

    # TODO: Verificar se é valida a remoção após processado
    os.remove(path)


def main(poll_interval=10):
    raw_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output', 'raw'))
    trusted_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output', 'trusted'))

    load_dotenv()

    user = os.getenv('DB_USER')
    pwd = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST', 'localhost')
    db = DatabaseConnection(user=user or '', password=pwd or '', host=host, database='health_data')
    db.open_connection()

    try:
        while True:
            files = list_raw_files(raw_dir)
            if not files:
                LOGGER.debug('Nenhum arquivo novo em %s', raw_dir)
            for f in files:
                try:
                    process_file(f, db, trusted_dir)
                except Exception as e:
                    LOGGER.exception('Erro processando %s: %s', f, e)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        LOGGER.info('Interrompido pelo usuário')
    finally:
        if getattr(db, 'connection', None):
            db.close_connection()


if __name__ == '__main__':
    main()
