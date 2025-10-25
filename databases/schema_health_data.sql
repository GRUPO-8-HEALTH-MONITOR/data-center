-- Script da Isa
CREATE DATABASE health_data;
USE health_data;


CREATE TABLE paciente (
  id int NOT NULL,
  nome varchar(60) DEFAULT NULL,
  altura decimal(10,2) DEFAULT NULL,
  peso decimal(10,2) DEFAULT NULL,
  dt_nasc date DEFAULT NULL,
  sexo char(1) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE doencas (
  id int NOT NULL,
  nome varchar(45) DEFAULT NULL,
  descricao varchar(200) DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE sensor (
  id int NOT NULL,
  nome varchar(45) DEFAULT NULL,
  descricao varchar(150) DEFAULT NULL,
  tipo_registro varchar(45) DEFAULT NULL,
  unidade_medida char(10) DEFAULT NULL,
  paciente_sensor_id int NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE paciente_sensor (
  id int NOT NULL,
  intervalo_captura int DEFAULT NULL,
  ativo tinyint DEFAULT NULL,
  dt_hr_inicio datetime DEFAULT NULL,
  dt_hr_fim datetime DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  paciente_id int NOT NULL,
  sensor_id int NOT NULL,
  PRIMARY KEY (id),
  KEY fk_paciente_sensor_Paciente1_idx (paciente_id),
  KEY fk_paciente_sensor_Sensor1_idx (sensor_id),
  CONSTRAINT fk_paciente_sensor_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id),
  CONSTRAINT fk_paciente_sensor_Sensor1 FOREIGN KEY (sensor_id) REFERENCES sensor (id)
);

CREATE TABLE registro (
  id int NOT NULL,
  valor decimal(6,2) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  paciente_sensor_id int NOT NULL,
  PRIMARY KEY (id),
  KEY fk_registro_paciente_sensor1_idx (paciente_sensor_id),
  CONSTRAINT fk_registro_paciente_sensor1 FOREIGN KEY (paciente_sensor_id) REFERENCES paciente_sensor (id)
);

CREATE TABLE medico (
  id int NOT NULL,
  nome varchar(60) DEFAULT NULL,
  crm char(10) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE doenca_paciente (
  doenca_id int NOT NULL,
  paciente_id int NOT NULL,
  observacao varchar(200) DEFAULT NULL,
  status varchar(30) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (doenca_id,paciente_id),
  KEY fk_Doencas_has_Paciente_Paciente1_idx (paciente_id),
  KEY fk_Doencas_has_Paciente_Doencas1_idx (doenca_id),
  CONSTRAINT fk_Doencas_has_Paciente_Doencas1 FOREIGN KEY (doenca_id) REFERENCES doencas (id),
  CONSTRAINT fk_Doencas_has_Paciente_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id)
);

CREATE TABLE diagnostico (
  paciente_id int NOT NULL,
  medico_id int NOT NULL,
  descricao varchar(100) DEFAULT NULL,
  dt_disgnostico date DEFAULT NULL,
  gravidade varchar(30) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (paciente_id,medico_id),
  KEY fk_Paciente_has_Medico1_Medico1_idx (medico_id),
  KEY fk_Paciente_has_Medico1_Paciente1_idx (paciente_id),
  CONSTRAINT fk_Paciente_has_Medico1_Medico1 FOREIGN KEY (medico_id) REFERENCES medico (id),
  CONSTRAINT fk_Paciente_has_Medico1_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id)
);

CREATE TABLE cirurgia (
  id int NOT NULL,
  paciente_id int NOT NULL,
  nome varchar(45) DEFAULT NULL,
  data_cirurgia date DEFAULT NULL,
  tempo_duracao time DEFAULT NULL,
  descricao varchar(200) DEFAULT NULL,
  satus varchar(45) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (id),
  KEY fk_Cirurgia_Paciente1_idx (paciente_id),
  CONSTRAINT fk_Cirurgia_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id)
);


CREATE TABLE medico_paciente (
  paciente_id int NOT NULL,
  medico_id int NOT NULL,
  Cirurgia_id int NOT NULL,
  dt_hr_inicio_atendimento date DEFAULT NULL,
  dt_hr_fim_atendimento datetime DEFAULT NULL,
  PRIMARY KEY (paciente_id,medico_id),
  KEY fk_Paciente_has_Medico_Medico1_idx (medico_id),
  KEY fk_Paciente_has_Medico_Paciente1_idx (paciente_id),
  KEY fk_Paciente_has_Medico_Cirurgia1_idx (Cirurgia_id),
  CONSTRAINT fk_Paciente_has_Medico_Cirurgia1 FOREIGN KEY (Cirurgia_id) REFERENCES cirurgia (id),
  CONSTRAINT fk_Paciente_has_Medico_Medico1 FOREIGN KEY (medico_id) REFERENCES medico (id),
  CONSTRAINT fk_Paciente_has_Medico_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id)
);


CREATE TABLE prontuario (
  id varchar(45) NOT NULL,
  medico_id int NOT NULL,
  paciente_id int NOT NULL,
  dt_hr_abertura datetime DEFAULT NULL,
  descricao varchar(45) DEFAULT NULL,
  status varchar(45) DEFAULT NULL,
  created_at datetime DEFAULT NULL,
  updated_at datetime DEFAULT NULL,
  PRIMARY KEY (id),
  KEY fk_Prontuario_Medico1_idx (medico_id),
  KEY fk_Prontuario_Paciente1_idx (paciente_id),
  CONSTRAINT fk_Prontuario_Medico1 FOREIGN KEY (medico_id) REFERENCES medico (id),
  CONSTRAINT fk_Prontuario_Paciente1 FOREIGN KEY (paciente_id) REFERENCES paciente (id)
);