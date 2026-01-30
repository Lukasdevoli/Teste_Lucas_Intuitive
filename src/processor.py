import pandas as pd
import zipfile
import os
import io
import re


RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def normalizar_colunas(df):
    
    df.columns = [col.strip().lower() for col in df.columns]
    
    mapa_colunas = {
        'reg_ans': 'registro_ans',
        'cd_conta_contabil': 'conta',
        'vl_saldo_final': 'valor',
        'descricao': 'descricao',
    }

    df.rename(columns=mapa_colunas, inplace=True)
    return df