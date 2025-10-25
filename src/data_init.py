"""
Gera dados simulados por paciente usando classes de sensores 
e salva blocos de JSONs em raw/

Funcionamento:
- Conecta ao banco. Se não conseguir conectar, roda em modo dry-run
  gerando N pacientes falsos.
- Busca os primeiros 100 pacientes e sensores associados.
- Usa multiprocessing.Pool para gerar dados em paralelo por paciente.
- Salva arquivos JSON por paciente em raw/paciente_{id}_{start}.json.

Config via env vars: DB_USER, DB_PASSWORD, DB_HOST (opcional)
"""
import os
import json
import logging
from time import sleep
from typing import List
from datetime import date
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count
from services.connection_database import DatabaseConnection

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] > %(name)s: %(message)s')

SENSOR_CLASS_MAP = {
	'frequencia': ('classes.frequencia_cardiaca', 'FreqCardiaca'),
	'glicose': ('classes.glicose', 'Glicose'),
	'movimentacao': ('classes.movimentacao', 'Movimentacao'),
	'temperatura': ('classes.temperatura_corporal', 'TemperaturaCorporal'),
	'pressao': ('classes.pressao_arterial', 'PressaoArterial'),
	'oxigenacao': ('classes.nivel_oxigenacao', 'NivelOxigenacao'),
	'umidade': ('classes.umidade_pele', 'UmidadePele')
}

# Correção para serializar Decimals e datetimes em JSON (IA)
class DecimalEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, Decimal):
			return float(obj)
		try:
			from datetime import datetime, date, time
			if isinstance(obj, (datetime, date, time)):
				return obj.isoformat()
			
		except Exception:
			pass
		return super(DecimalEncoder, self).default(obj)

def import_sensor_class(sensor_name: str):
	key = sensor_name.lower()
	for k, (mod, cls) in SENSOR_CLASS_MAP.items():
		if k in key:
			module = __import__(mod, fromlist=[cls])
			return getattr(module, cls)()
	
    # fallback
	try:
		module = __import__(f"classes.{key}", fromlist=['*'])
		for attr in dir(module):
			if attr[0].isupper():
				return getattr(module, attr)()
	except Exception:
		LOGGER.warning(f"Classe do sensor {sensor_name} não encontrada")
	return None


def generate_patient_data(args):
	paciente, sensors_info, duration_minutes, interval_seconds, output_dir = args
	pid = paciente.get('id')
	LOGGER.info("Gerando dados para paciente %s", pid)

	all_records = []
	for s in sensors_info:
		sensor_name = s.get('nome') or s.get('sensor_nome', '')
		sensor_inst = import_sensor_class(sensor_name)
		infos_medica = {
			'idade': paciente.get('idade') if 'idade' in paciente else 45,
			'peso': paciente.get('peso'),
			'altura': paciente.get('altura')
		}
		if sensor_inst is None:
			LOGGER.debug("Nenhuma classe para sensor %s - pulando", sensor_name)
			continue
		try:
			recs = sensor_inst.start(infos_medica, duration_minutes=duration_minutes, interval_seconds=interval_seconds)
			if isinstance(recs, list):
				all_records.extend(recs)
				
		except Exception as e:
			LOGGER.exception("Erro gerando dados sensor %s: %s", sensor_name, e)

	start_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
	filename = f"paciente_{pid}_{start_ts}.json"
	os.makedirs(output_dir, exist_ok=True)
	out_path = os.path.join(output_dir, filename)

	payload = {
		'paciente': paciente,
		'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
		'records': all_records
	}

	tmp_path = out_path + '.tmp'
	with open(tmp_path, 'w', encoding='utf-8') as tf:
		json.dump(payload, tf, ensure_ascii=False, indent=2, cls=DecimalEncoder)
		tf.flush()
		try:
			os.fsync(tf.fileno())
		except Exception:
			pass

	max_attempts = 5
	base_delay = 0.2
	moved = False
	for attempt in range(1, max_attempts + 1):
		try:
			os.replace(tmp_path, out_path)
			moved = True
			break
		except PermissionError as e:
			LOGGER.warning('Tentativa %d/%d: falha ao mover %s -> %s: %s', attempt, max_attempts, tmp_path, out_path, e)
			if attempt < max_attempts:
				sleep(base_delay * attempt)
				continue
			LOGGER.exception('Não foi possível mover %s para %s após %d tentativas', tmp_path, out_path, max_attempts)
			failed_path = out_path + '.failed'
			try:
				os.replace(tmp_path, failed_path)
				LOGGER.error('Temp movido para %s para inspeção', failed_path)
				return failed_path
			except Exception:
				try:
					os.remove(tmp_path)
				except Exception:
					LOGGER.exception('Não foi possível remover o arquivo temporário %s', tmp_path)
				return None

	if moved:
		LOGGER.info('Arquivo salvo: %s (registros: %d)', out_path, len(all_records))
		return out_path
	else:
		return None


def fetch_first_n_patients(db: DatabaseConnection, n=100) -> List[dict]:
	if not db or not getattr(db, 'connection', None):
		LOGGER.warning('DB não disponível — modo dry-run: gerando pacientes falsos')
		fake = []
		for i in range(1, min(n, 10) + 1):
			fake.append({'id': i, 'nome': f'Paciente {i}', 'idade': 40 + (i % 30), 'altura': 1.7, 'peso': 70})
		return fake

	cursor = db.connection.cursor(dictionary=True)
	cursor.execute('SELECT id, nome, altura, peso, dt_nasc, sexo FROM paciente ORDER BY id LIMIT %s', (n,))
	rows = cursor.fetchall()
	for r in rows:
		try:
			if r.get('dt_nasc'):
				age = date.today().year - r['dt_nasc'].year
				r['idade'] = age
		except Exception:
			r['idade'] = 45
	cursor.close()
	return rows


def fetch_sensors_for_patient(db: DatabaseConnection, paciente_id: int):
	if not db or not getattr(db, 'connection', None):
		# default set
		return [
			{'sensor_id': 1, 'nome': 'frequencia_cardiaca'},
			{'sensor_id': 2, 'nome': 'glicose'},
			{'sensor_id': 3, 'nome': 'temperatura_corporal'}
		]

	cursor = db.connection.cursor(dictionary=True)
	query = ("SELECT ps.id as paciente_sensor_id, s.id as sensor_id, s.nome, s.tipo_registro, s.unidade_medida "
			 "FROM paciente_sensor ps JOIN sensor s ON ps.sensor_id = s.id WHERE ps.paciente_id = %s")
	cursor.execute(query, (paciente_id,))
	rows = cursor.fetchall()
	cursor.close()
	return rows


def main():
	load_dotenv()
	user = os.getenv('DB_USER')
	pwd = os.getenv('DB_PASSWORD')
	host = os.getenv('DB_HOST', 'localhost')
	output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'output', 'raw'))

	db = DatabaseConnection(user=user or '', password=pwd or '', host=host, database='health_data')
	db.open_connection()

	generation_interval_seconds = 10
	sensor_interval_seconds = 1
	batch_duration_seconds = 10

	duration_minutes = batch_duration_seconds / 60.0
	interval_seconds = sensor_interval_seconds

	continuous = bool(getattr(db, 'connection', None))

	processes = min(8, max(1, cpu_count()))
	LOGGER.info('Usando %d processos; modo contínuo=%s', processes, continuous)

	try:
		if continuous:
			# Pool contínuo 
			with Pool(processes=processes) as pool:
				LOGGER.info('Iniciando loop contínuo de geração (pressione Ctrl+C para parar)')
				while True:
					patients = fetch_first_n_patients(db, n=100)
					LOGGER.info('Pacientes a processar neste ciclo: %d', len(patients))
					tasks = []
					for p in patients:
						sensors = fetch_sensors_for_patient(db, p.get('id'))
						tasks.append((p, sensors, duration_minutes, interval_seconds, output_dir))

					results = pool.map(generate_patient_data, tasks)
					LOGGER.info('Ciclo concluído. Arquivos gerados: %d', len(results))
					sleep(generation_interval_seconds)
		else:
			# modo dry-run 
			LOGGER.info('Entrando em loop dry-run (gerando lotes a cada %ds)', generation_interval_seconds)
			with Pool(processes=processes) as pool:
				while True:
					patients = fetch_first_n_patients(db, n=100)
					LOGGER.info('Pacientes a processar: %d', len(patients))
					tasks = []
					for p in patients:
						sensors = fetch_sensors_for_patient(db, p.get('id'))
						tasks.append((p, sensors, duration_minutes, interval_seconds, output_dir))

					LOGGER.info('Iniciando pool com %d processos', processes)
					results = pool.map(generate_patient_data, tasks)
					LOGGER.info('Geração do lote concluída. Arquivos: %s', results)
					sleep(generation_interval_seconds)
	except KeyboardInterrupt:
		LOGGER.info('Execução interrompida pelo usuário')
	finally:
		if getattr(db, 'connection', None):
			db.close_connection()


if __name__ == '__main__':
	main()



