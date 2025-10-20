import os
import pandas as pd


def ler_arquivo_local(path: str, sep: str) -> pd.DataFrame:
    """Lê CSV ou Excel com flexibilidade"""
    print(f"\n📖 - Lendo arquivo {path}...")
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        try:
            return pd.read_csv(path, sep=sep)
        except pd.errors.ParserError:
            print("⚠️ - Erro ao ler CSV com ';', tentando ','...")
    else:
        raise ValueError(f"Formato de arquivo não suportado: {ext}")
