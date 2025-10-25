# 🎲🔍 Data Center
## Pipeline de geração e ingestão de sinais vitais

Projeto para gerar dados simulados de sensores por paciente (producer) e consumir esses arquivos para limpeza/entrega (consumer).

Estrutura principal
- `src/data_init.py` — produtor: gera lotes por paciente e grava JSONs em `output/raw/`.
- `src/process_and_save.py` — consumidor: lê `output/raw/`, normaliza com o "Spark", grava `output/trusted/` e insere em banco (`registro`).
- `src/classes/*` — classes de sensores (cada `start(infos_medica, duration_minutes, interval_seconds)` retorna lista de registros).
- `src/services/connection_database.py` — helper para conexão MySQL (opcional; suporta dry-run quando o conector não está configurado).
- `databases/schema_health_data.sql` e `databases/seed_health_data.sql` — schema e dados de exemplo (100 pacientes, sensores, mapeamentos).

Visão geral do fluxo
- Producer: por padrão gera lotes a cada 10 segundos; cada lote cobre 10 segundos com amostras a cada 1 segundo (10 pontos por sensor). Grava arquivos atômicos em `raw/`.
- Consumer: verifica `raw/`, valida e normaliza os registros, salva arquivos limpos em `trusted/` e insere os registros válidos no banco (somente quando houver mapeamento `paciente_sensor`).

Requisitos
- Python 3.9+ (o projeto foi testado com 3.11)
- (opcional, para inserir no MySQL) `mysql-connector-python`
- As dependências estão listadas em `requirements.txt`.

Setup rápido (Windows PowerShell)
1) Crie e ative virtualenv:
```powershell
python -m venv .\venv
.\venv\Scripts\Activate.ps1
```
2) Instale dependências:
```powershell
pip install -r requirements.txt
```
3) Configure variáveis de ambiente (opcional se rodar em dry-run):
```powershell
$env:DB_USER = 'seu_usuario'
$env:DB_PASSWORD = 'sua_senha'
$env:DB_HOST = 'localhost'
```

Banco de dados
- Para usar persistência no MySQL importe o schema:

  1. Crie o banco e tabelas executando `databases/schema_health_data.sql` no seu servidor MySQL.
  2. (Opcional) Popule com `databases/seed_health_data.sql`.

Como rodar
- Producer (gera lotes a cada 10s):
```powershell
.\venv\Scripts\python.exe -u src\data_init.py
```

- Consumer (roda continuamente, processa arquivos em `output/raw`):
```powershell
.\venv\Scripts\python.exe -u src\process_and_save.py
```

Arquivos de saída
- `output/raw/` — arquivos JSON brutos por paciente (escritos atômicamente; extensão temporária `.tmp` usada durante gravação).
- `output/trusted/` — arquivos JSON já normalizados prontos para ingestão.

Comportamento de janelas e frequência
- O produtor gera um lote a cada 10 segundos (parâmetro `generation_interval_seconds` dentro de `src/data_init.py`).
- Cada sensor é amostrado a cada 1 segundo (parâmetro `sensor_interval_seconds`), produzindo ~10 registros por sensor por lote.

Resolução de problemas comuns
- PermissionError no Windows ao renomear `.tmp` -> `.json`:
  - Mensagem: `PermissionError: [WinError 5] Acesso negado`.
  - Causa: outro processo estava mantendo o arquivo destino aberto. Já implementamos retry/backoff e gravação atômica para reduzir esse problema.

- Arquivos truncados antigos em `output/raw/`:
  - Arquivos gerados antes da correção atômica podem estar truncados; remova ou mova os arquivos truncados para `output/raw/broken/` antes de rodar o consumer.

- Inserções retornando 1364 / 'doesn't have a default value':
  - Veja seção Banco de dados acima para aplicar `AUTO_INCREMENT` nas colunas `id`.

Melhorias que já estão implementadas
- Escrita atômica (tmp + fsync + os.replace com retry)
- Consumer com retry na leitura JSON e movimentação de arquivos corrompidos para `output/raw/broken`
- Prefetch de sensores e mapeamentos no consumer para reduzir consultas por linha
