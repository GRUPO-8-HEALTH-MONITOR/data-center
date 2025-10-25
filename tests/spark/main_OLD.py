import os
import sys
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# TODO: Ver com o pessoal para separar em um arquivo de classe
class HealthMonitorDataProcessor:
    def __init__(self):
        #Spark no Windows
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        
        self.spark = SparkSession.builder \
            .appName("HealthMonitorSensorData") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.default.parallelism", "1") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.sql.adaptive.skewJoin.enabled", "false") \
            .config("spark.ui.enabled", "false") \
            .master("local[1]") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        self.sensor_schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("patient_id", StringType(), True),
            StructField("sensor_type", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("device_id", StringType(), True)
        ])
    
    def capture_from_s3(self, bucket_name, file_path):
        try:
            s3_path = f"s3a://{bucket_name}/{file_path}"
            df = self.spark.read.option("header", "true").csv(s3_path)
            print(f"Data captured from S3: {s3_path}")
            return df
        except Exception as e:
            print(f"Error capturing data from S3: {e}")
            return None
    
    def capture_from_url(self, url, local_path="temp_data.csv"):
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            df = self.spark.read.option("header", "true").csv(local_path)
            print(f"Data captured from URL: {url}")
            return df
        except Exception as e:
            print(f"Error capturing data from URL: {e}")
            return None
    
    def capture_from_local(self, file_path):
        try:
            df = self.spark.read.option("header", "true").csv(file_path)
            print(f"Data captured from local file: {file_path}")
            return df
        except Exception as e:
            print(f"Error capturing data from local file: {e}")
            return None
    
    def process_heart_rate_data(self, df):
        """Frequência cardíaca"""
        processed_df = df.withColumn("sensor_type", lit("heart_rate")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value").between(40, 200))  # Valores normais de BPM
        
        return processed_df
    
    def process_glucose_data(self, df):
        """Glicose"""
        processed_df = df.withColumn("sensor_type", lit("glucose")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value").between(70, 400))  # mg/dL
        
        return processed_df
    
    def process_blood_pressure_data(self, df):
        """pressão arterial"""
        processed_df = df.withColumn("sensor_type", lit("blood_pressure")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value").between(60, 300))  # mmHg
        
        return processed_df
    
    def process_temperature_data(self, df):
        """temperatura corporal"""
        processed_df = df.withColumn("sensor_type", lit("body_temperature")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value").between(35.0, 42.0))  # Celsius
        
        return processed_df
    
    def process_oxygen_data(self, df):
        """oxigenação"""
        processed_df = df.withColumn("sensor_type", lit("oxygen_saturation")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value").between(80, 100))  # Porcentagem
        
        return processed_df
    
    def process_movement_data(self, df):
        """movimentação"""
        processed_df = df.withColumn("sensor_type", lit("movement")) \
                        .withColumn("timestamp", current_timestamp()) \
                        .filter(col("value") >= 0)
        
        return processed_df
    
    def unify_sensor_data(self, dataframes_list):
        if not dataframes_list:
            return None
        
        unified_df = dataframes_list[0]
        for df in dataframes_list[1:]:
            unified_df = unified_df.union(df)
        
        unified_df = unified_df.withColumn("processed_at", current_timestamp()) \
                              .withColumn("data_quality", lit("validated"))
        
        return unified_df
    
    # SUGESTÃO DE IA
    def generate_health_summary(self, df):
        """Gera resumo"""
        summary = df.groupBy("patient_id", "sensor_type") \
                   .agg(
                       avg("value").alias("avg_value"),
                       min("value").alias("min_value"),
                       max("value").alias("max_value"),
                       count("value").alias("reading_count"),
                       max("timestamp").alias("last_reading")
                   )
        
        return summary
    
    def save_data(self, df, output_path, format_type="csv"):
        try:
            pandas_df = df.toPandas()
            if format_type == "csv":
                file_path = output_path + ".csv" if not output_path.endswith('.csv') else output_path
                pandas_df.to_csv(file_path, index=False)
                print(f"Data saved in: {file_path}")
            else:
                file_path = output_path + ".parquet" if not output_path.endswith('.parquet') else output_path
                pandas_df.to_parquet(file_path, index=False)
                print(f"Data saved in: {file_path}")

        except Exception as e:
            print(f"Error with pandas: {e}")
            return None

    def run_health_monitor_pipeline(self):
        print("=== INICIANDO PIPELINE DE MONITORAMENTO DE SAÚDE ===")
        processed_dataframes = []
        
        # TODO: No momento é simulação - Puxar com API ou S3
        print("\n1. Capturando dados de sensores...")
        
        base_time = datetime(2024, 8, 10, 10, 0, 0)
        
        # PS. Exemplo gerado com IA
        sample_data = [
            (base_time, "PAC001", "heart_rate", 75.0, "bpm", "DEV001"),
            (base_time, "PAC001", "glucose", 95.0, "mg/dL", "DEV002"),
            (base_time, "PAC001", "blood_pressure", 120.0, "mmHg", "DEV003"),
            (base_time + timedelta(minutes=1), "PAC002", "heart_rate", 82.0, "bpm", "DEV004"),
            (base_time + timedelta(minutes=1), "PAC002", "temperature", 36.5, "C", "DEV005"),
        ]
        df_sample = self.spark.createDataFrame(sample_data, self.sensor_schema)

        print("\n2. Processando dados por tipo de sensor...")
        sensor_types = [
            ("heart_rate", self.process_heart_rate_data),
            ("glucose", self.process_glucose_data),
            ("blood_pressure", self.process_blood_pressure_data),
            ("temperature", self.process_temperature_data),
            ("oxygenation", self.process_oxygen_data),
            ("movement", self.process_movement_data)
        ]

        for sensor_type, process_function in sensor_types:
            sensor_df = df_sample.filter(col("sensor_type") == sensor_type)
            if sensor_df.count() > 0:
                processed_df = process_function(sensor_df)
                processed_dataframes.append(processed_df)
                print(f"Processados {processed_df.count()} registros de {sensor_type}")

        print("\n3. Unificando dados de sensores...")
        unified_data = self.unify_sensor_data(processed_dataframes)
        if unified_data:
            print(f"Total de registros unificados: {unified_data.count()}")

            print("\n4. Amostra dos dados processados:")
            unified_data.show(10, truncate=False)

            print("\n5. Gerando resumo de saúde por paciente...")
            health_summary = self.generate_health_summary(unified_data)
            health_summary.show()
            
            print("\n6. Salvando dados processados...")
            self.save_data(unified_data, "output/unified_health_data", format_type='csv')
            self.save_data(health_summary, "output/health_summary", format_type='csv')

        print("\n=== PIPELINE CONCLUÍDO ===")
    
    def stop_spark(self):
        self.spark.stop()

if __name__ == "__main__":
    processor = HealthMonitorDataProcessor()
    
    processor.run_health_monitor_pipeline()
    
    processor.stop_spark()