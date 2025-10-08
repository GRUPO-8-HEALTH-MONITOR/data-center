import os
import csv
import time
import json
import tracemalloc


def get_configs()-> list:
    path_src = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(path_src, '..', '..', 'config', 'config.json')

    if not os.path.exists(config_file):
        Exception('Config file not found.')

    return json.load(open(config_file))


def measure_performance(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        tracemalloc.start()

        result = func(*args, **kwargs)

        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        execution_time = (end_time - start_time) * 1000
        current_memory = current / 10**3
        peak_memory = peak / 10**3

        print(f"[?] Função '{func.__name__}' executada em {execution_time:.4f} milissegundos")
        print(f"[?] Uso atual da memória: {current_memory:.4f} KB; Pico: {peak_memory:.4f} KB")

        csv_file_path = 'reports/performance_data.csv'

        file_exists = os.path.isfile(csv_file_path)

        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)

            if not file_exists:
                writer.writerow(['Function', 'Execution Time (ms)', 'Current Memory (KB)', 'Peak Memory (KB)'])
            
            writer.writerow([func.__name__, execution_time, current_memory, peak_memory])

        return result

    return wrapper


def measure_sensor_performance(func):
    """
    Decorator to measure the performance of each sensor during value generation.
    """
    def wrapper(sensor, *args, **kwargs):
        print("=-" * 20)
        print(f"Measuring performance for sensor: {sensor.get('name')}")

        start_time = time.time()
        result = func(sensor, *args, **kwargs)
        end_time = time.time()  

        elapsed_time = (end_time - start_time) * 1000 
        avg_value = sum(result) / len(result) if result else 0
        min_value = min(result) if result else 0
        max_value = max(result) if result else 0
        data_count = len(result)  

        print(f"Sensor {sensor.get('name')} performance:")
        print(f"  - Time taken: {elapsed_time:.2f} ms")
        print(f"  - Average value: {avg_value:.2f}")
        print(f"  - Min value: {min_value:.2f}")
        print(f"  - Max value: {max_value:.2f}")
        print(f"  - Data points processed: {data_count}")

        csv_file_path = 'reports/sensor_performance.csv'
        file_exists = os.path.isfile(csv_file_path)

        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(['Sensor', 'Time (ms)', 'Average Value', 'Min Value', 'Max Value', 'Data Count'])
            writer.writerow([sensor.get('name'), elapsed_time, avg_value, min_value, max_value, data_count])

        return result

    return wrapper


def load_config():
    """
    Load configuration from the config.json file.
    
    Returns:
        dict: Configuration dictionary.
    """
    path_src = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(path_src, '..', '..', 'config', 'config.json')

    if not os.path.exists(config_file):
        raise FileNotFoundError('Config file not found.')

    with open(config_file, 'r') as file:
        return json.load(file)