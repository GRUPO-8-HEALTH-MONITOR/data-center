import os
from datetime import datetime 
#from pyspark.sql import SparkSession # Releva
from step3_data_delivery import DataDelivery
from step1_data_ingestion import DataIngestion
from step2_data_processing import DataProcessing

class HealthMonitorPipeline:
    def __init__(self):
        self.ingestion = DataIngestion()
        self.processing = DataProcessing()
        self.delivery = DataDelivery()
    
    def execute_full_pipeline(self, collection_minutes=5, delivery_to_cloud=False):
        print("=== HEALTH MONITOR PIPELINE ===")
        print(f"Iniciando pipeline completo - {datetime.now()}")
        
        print("\n--- STEP 1: DATA INGESTION ---")
        raw_data_file = self.ingestion.executar_coleta(
            duration_minutes=collection_minutes,
            interval_seconds=2
        )
        
        print("\n--- STEP 2: DATA PROCESSING ---")
        processed_paths = self.processing.processar_dados_completo(raw_data_file)
        
        print("\n--- STEP 3: DATA DELIVERY ---")
        if delivery_to_cloud:
            self.delivery.delivery_config.update({
                'aws_s3': True,
                'azure_blob': True,
                'gcp_storage': True
            })
        
        delivery_results = self.delivery.deliver_processed_data(processed_paths)
        
        self.processing.stop_spark()
        
        print("\n=== PIPELINE CONCLUÍDO ===")
        print(f"Arquivos processados: {list(processed_paths.keys())}")
        print(f"Entregas realizadas: {len(delivery_results)}")
        
        return {
            "raw_data": raw_data_file,
            "processed_data": processed_paths,
            "delivery_results": delivery_results
        }

if __name__ == "__main__":
    pipeline = HealthMonitorPipeline()
    
    results = pipeline.execute_full_pipeline(
        collection_minutes=3,
        delivery_to_cloud=False
    )
    
    print(f"\nResultados finais: {results}")
