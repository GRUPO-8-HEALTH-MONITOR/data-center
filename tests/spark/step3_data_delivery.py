import os
import json
import glob
import boto3
import shutil
from datetime import datetime

class DataDelivery:
    def __init__(self):
        self.aws_bucket_name = os.getenv('AWS_BUCKET_NAME', 'health-monitor-data')
        self.azure_container_name = os.getenv('AZURE_CONTAINER_NAME', 'health-data')
        self.gcp_bucket_name = os.getenv('GCP_BUCKET_NAME', 'health-monitor-gcp')
        
        self.delivery_config = {
            'local_backup': True,
            'aws_s3': False,
            'azure_blob': False,
            'gcp_storage': False
        }

    def setup_aws_client(self):
        if not self.delivery_config['aws_s3']:
            return None
        
        try:
            return boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
        except Exception as e:
            print(f"Erro ao configurar AWS S3: {e}")
            return None

    def upload_to_aws_s3(self, local_path, s3_key):
        s3_client = self.setup_aws_client()
        if not s3_client:
            return False
        
        try:
            if os.path.isdir(local_path):
                for root, dirs, files in os.walk(local_path):
                    for file in files:
                        local_file = os.path.join(root, file)
                        relative_path = os.path.relpath(local_file, local_path)
                        s3_file_key = f"{s3_key}/{relative_path}".replace("\\", "/")
                        s3_client.upload_file(local_file, self.aws_bucket_name, s3_file_key)
            else:
                s3_client.upload_file(local_path, self.aws_bucket_name, s3_key)
            
            print(f"Upload para S3 concluído: s3://{self.aws_bucket_name}/{s3_key}")
            return True
        except Exception as e:
            print(f"Erro no upload para S3: {e}")
            return False

    def create_local_backup(self, source_path, backup_dir="../../artifacts"):
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if os.path.isdir(source_path):
            backup_path = os.path.join(backup_dir, f"backup_{timestamp}")
            shutil.copytree(source_path, backup_path)
        else:
            filename = os.path.basename(source_path)
            backup_path = os.path.join(backup_dir, f"backup_{timestamp}_{filename}")
            shutil.copy2(source_path, backup_path)
        
        print(f"Backup local criado: {backup_path}")
        return backup_path

    def deliver_processed_data(self, data_paths, delivery_prefix="health_data"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        delivery_results = {}
        
        for data_type, path in data_paths.items():
            if not os.path.exists(path):
                print(f"Caminho não encontrado: {path}")
                continue
            
            remote_key = f"{delivery_prefix}/{data_type}/{timestamp}"
            
            if self.delivery_config['local_backup']:
                backup_path = self.create_local_backup(path)
                delivery_results[f"{data_type}_local"] = backup_path
            
            if self.delivery_config['aws_s3']:
                success = self.upload_to_aws_s3(path, remote_key)
                delivery_results[f"{data_type}_aws"] = success
        
        return delivery_results

    def cleanup_local_files(self, paths, keep_backup=True):
        for path in paths.values():
            if os.path.exists(path) and not keep_backup:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                print(f"Arquivo local removido: {path}")

    def generate_delivery_report(self, delivery_results, output_dir="../../output"):
        report = {
            "delivery_timestamp": datetime.now().isoformat(),
            "results": delivery_results,
            "total_deliveries": len(delivery_results),
            "successful_deliveries": sum(1 for v in delivery_results.values() if v is True or isinstance(v, str))
        }
        
        report_path = os.path.join(output_dir, f"delivery_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"Relatório de entrega gerado: {report_path}")
        return report_path

    def execute_delivery_pipeline(self, processed_data_dir="../../output"):
        print("Iniciando pipeline de entrega de dados...")
        
        latest_files = {}
        patterns = {
            "processed_data": "processed_health_data_*",
            "statistics": "health_statistics_*", 
            "alerts": "health_alerts_*"
        }
        
        for data_type, pattern in patterns.items():
            files = glob.glob(os.path.join(processed_data_dir, pattern))
            if files:
                latest_files[data_type] = max(files, key=os.path.getctime)
        
        if not latest_files:
            print("Nenhum arquivo processado encontrado.")
            return
        
        print(f"Arquivos encontrados para entrega: {latest_files}")
        
        delivery_results = self.deliver_processed_data(latest_files)
        
        report_path = self.generate_delivery_report(delivery_results)
        
        print("Pipeline de entrega concluído!")
        return delivery_results

if __name__ == "__main__":
    delivery = DataDelivery()
    
    delivery.delivery_config['local_backup'] = True
    
    results = delivery.execute_delivery_pipeline()
    print(f"Resultados da entrega: {results}")
