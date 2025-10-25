import os
import json
from datetime import datetime
from pyspark.sql.types import * # type: ignore
from pyspark.sql.functions import * # type: ignore
from pyspark.sql import SparkSession

class DataProcessing:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("HealthDataProcessing") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.health_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("bpm", IntegerType(), True),
            StructField("spo2", DoubleType(), True),
            StructField("glicose", DoubleType(), True),
            StructField("pressao_sistolica", IntegerType(), True),
            StructField("pressao_diastolica", IntegerType(), True),
            StructField("aceleracao_x", DoubleType(), True),
            StructField("aceleracao_y", DoubleType(), True),
            StructField("aceleracao_z", DoubleType(), True),
            StructField("giroscopio_x", DoubleType(), True),
            StructField("giroscopio_y", DoubleType(), True),
            StructField("giroscopio_z", DoubleType(), True),
            StructField("magnitude_aceleracao", DoubleType(), True),
            StructField("temperatura", DoubleType(), True),
            StructField("umidade_pele", DoubleType(), True)
        ])

    def carregar_dados_raw(self, input_path):
        if os.path.isfile(input_path):
            with open(input_path, 'r') as f:
                data = json.load(f)
            df = self.spark.createDataFrame(data, self.health_schema)
        else:
            df = self.spark.read.option("multiline", "true").json(input_path, schema=self.health_schema)
        
        return df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    def processar_metricas_vitais(self, df):
        df_processed = df.withColumn("pressao_media", (col("pressao_sistolica") + col("pressao_diastolica")) / 2) \
                        .withColumn("status_bpm", 
                                   when(col("bpm") < 60, "baixo")
                                   .when(col("bpm") > 100, "alto")
                                   .otherwise("normal")) \
                        .withColumn("status_spo2",
                                   when(col("spo2") < 95, "baixo")
                                   .when(col("spo2") >= 98, "normal")
                                   .otherwise("moderado")) \
                        .withColumn("status_glicose",
                                   when(col("glicose") < 70, "hipoglicemia")
                                   .when(col("glicose") > 140, "hiperglicemia")
                                   .otherwise("normal")) \
                        .withColumn("status_pressao",
                                   when(col("pressao_sistolica") < 90, "baixa")
                                   .when(col("pressao_sistolica") > 140, "alta")
                                   .otherwise("normal"))
        
        return df_processed

    def detectar_anomalias_movimento(self, df):
        df_movimento = df.withColumn("atividade_nivel",
                                   when(col("magnitude_aceleracao") < 0.5, "repouso")
                                   .when(col("magnitude_aceleracao") < 2.0, "movimento_leve")
                                   .when(col("magnitude_aceleracao") < 5.0, "movimento_moderado")
                                   .otherwise("movimento_intenso"))
        
        return df_movimento

    def calcular_estatisticas_janela(self, df, window_minutes=10):
        window_spec = window(col("timestamp"), f"{window_minutes} minutes")
        
        stats_df = df.groupBy(window_spec) \
                    .agg(
                        avg("bpm").alias("bpm_media"),
                        stddev("bpm").alias("bpm_desvio"),
                        min("bpm").alias("bpm_min"),
                        max("bpm").alias("bpm_max"),
                        avg("spo2").alias("spo2_media"),
                        avg("glicose").alias("glicose_media"),
                        avg("pressao_media").alias("pressao_media_avg"),
                        avg("temperatura").alias("temperatura_media"),
                        avg("umidade_pele").alias("umidade_media"),
                        count("*").alias("total_registros")
                    ) \
                    .withColumn("janela_inicio", col("window.start")) \
                    .withColumn("janela_fim", col("window.end")) \
                    .drop("window")
        
        return stats_df

    def gerar_alertas(self, df):
        alertas_df = df.filter(
            (col("status_bpm") != "normal") |
            (col("status_spo2") == "baixo") |
            (col("status_glicose") != "normal") |
            (col("status_pressao") != "normal")
        ).withColumn("alerta_timestamp", current_timestamp()) \
         .withColumn("severidade",
                    when((col("spo2") < 90) | (col("glicose") < 50) | (col("bpm") > 120), "critico")
                    .when((col("spo2") < 95) | (col("glicose") > 200) | (col("bpm") < 50), "alto")
                    .otherwise("moderado"))
        
        return alertas_df

    def processar_dados_completo(self, input_path, output_dir="../../output"):
        print("Carregando dados raw...")
        df_raw = self.carregar_dados_raw(input_path)
        
        print("Processando métricas vitais...")
        df_processed = self.processar_metricas_vitais(df_raw)
        
        print("Detectando anomalias de movimento...")
        df_movimento = self.detectar_anomalias_movimento(df_processed)
        
        print("Calculando estatísticas por janela de tempo...")
        df_stats = self.calcular_estatisticas_janela(df_movimento)
        
        print("Gerando alertas...")
        df_alertas = self.gerar_alertas(df_movimento)
        
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        output_processed = os.path.join(output_dir, f"processed_health_data_{timestamp}")
        output_stats = os.path.join(output_dir, f"health_statistics_{timestamp}")
        output_alertas = os.path.join(output_dir, f"health_alerts_{timestamp}")
        
        print("Salvando dados processados...")
        df_movimento.coalesce(1).write.mode("overwrite").parquet(output_processed)
        df_stats.coalesce(1).write.mode("overwrite").parquet(output_stats)
        df_alertas.coalesce(1).write.mode("overwrite").parquet(output_alertas)
        
        print(f"Dados processados salvos em: {output_processed}")
        print(f"Estatísticas salvas em: {output_stats}")
        print(f"Alertas salvos em: {output_alertas}")
        
        return {
            "processed_data": output_processed,
            "statistics": output_stats,
            "alerts": output_alertas
        }

    def stop_spark(self):
        self.spark.stop()

if __name__ == "__main__":
    processor = DataProcessing()
    
    input_files = "../../output/raw_health_data_*.json"
    
    try:
        results = processor.processar_dados_completo(input_files)
        print("Processamento concluído com sucesso!")
        print(f"Resultados: {results}")
    finally:
        processor.stop_spark()
