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
        'registro_ans': 'registro_ans',
        'cd_operadora': 'registro_ans',
    
        'cd_conta_contabil': 'conta',
        'cd_conta': 'conta',
        'conta': 'conta',
        
        'vl_saldo_final': 'valor',
        'vl_saldo_inicial': 'valor_inicial', 
        'saldo_final': 'valor',
        'valor': 'valor',
        'vl_saldo': 'valor',
        
        'descricao': 'descricao',
        'ds_conta': 'descricao',
        'nm_conta_contabil': 'descricao'
    }

    
    df.rename(columns=mapa_colunas, inplace=True)
    
    
    if 'valor' not in df.columns:
        print(f"ALERTA: Coluna de VALOR não encontrada! Colunas disponíveis: {list(df.columns)}")
        
    return df


def encontrar_arquivo_csv(zip_ref):
   
    for nome_arquivo in zip_ref.namelist():

        if nome_arquivo.endswith('/') or '__MACOSX' in nome_arquivo:
            continue
        
        if nome_arquivo.lower().endswith('.csv') or nome_arquivo.lower().endswith('.txt'):
            return nome_arquivo
    return None


def processar_arquivos():
    print(">>> Iniciando processamento de dados (ETL)...")
    
    if not os.path.exists(RAW_DIR):
        print(f"Pasta {RAW_DIR} não encontrada. Rode o scraper primeiro.")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    dataframes = []
    arquivos_zip = [f for f in os.listdir(RAW_DIR) if f.endswith('.zip')]
    
    if not arquivos_zip:
        print("Nenhum arquivo ZIP encontrado para processar.")
        return

    for arquivo in arquivos_zip:
        caminho_completo = os.path.join(RAW_DIR, arquivo)
        print(f"Processando {arquivo}...")
        
        partes = arquivo.split('_')
        ano = partes[0]
        trimestre = partes[1].replace('T', '').replace('Q', '') 

        try:
            with zipfile.ZipFile(caminho_completo, 'r') as z:
                arquivo_alvo = encontrar_arquivo_csv(z)
                
                if not arquivo_alvo:
                    print(f"  -> Nenhum CSV encontrado dentro de {arquivo}")
                    continue
                
                print(f"  -> Lendo arquivo interno: {arquivo_alvo}")
                
                with z.open(arquivo_alvo) as f:

                    df = pd.read_csv(
                        f, 
                        sep=';', 
                        encoding='latin1', 
                        dtype=str, 
                        on_bad_lines='skip'
                    )
                    
                    df = normalizar_colunas(df)
                    
                    col_desc = next((c for c in df.columns if 'desc' in c), None)
                    
                    if col_desc:
                        filtro = df[col_desc].str.contains('EVENTO|SINISTRO|DESPESA', case=False, na=False)
                        df = df[filtro]
                    
                    df['ano'] = ano
                    df['trimestre'] = trimestre
                    
                    dataframes.append(df)
                    print(f"  -> {len(df)} linhas extraídas.")

        except Exception as e:
            print(f"  -> Erro ao processar {arquivo}: {e}")

    if not dataframes:
        print("Nenhum dado foi extraído.")
        return

    print("Consolidando DataFrames...")
    df_final = pd.concat(dataframes, ignore_index=True)
    
    if 'valor' in df_final.columns:
        df_final['valor'] = df_final['valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df_final['valor'] = pd.to_numeric(df_final['valor'], errors='coerce')
 
    colunas_finais = {
        'valor': 'Valor Despesas',
        'ano': 'Ano',
        'trimestre': 'Trimestre',

        'cnpj': 'CNPJ',
        'razao_social': 'RazaoSocial'
    }


    if 'registro_ans' in df_final.columns:
        df_final.rename(columns={'registro_ans': 'RegistroANS'}, inplace=True)
    elif 'reg_ans' in df_final.columns:
        df_final.rename(columns={'reg_ans': 'RegistroANS'}, inplace=True)
    else:
        df_final['RegistroANS'] = None

    for col_old, col_new in colunas_finais.items():
        if col_old not in df_final.columns:
            df_final[col_new] = None 
        else:
            df_final.rename(columns={col_old: col_new}, inplace=True)

    cols_to_save = ['RegistroANS', 'Ano', 'Trimestre', 'Valor Despesas']

    df_final = df_final[cols_to_save]
    
    output_path = os.path.join(PROCESSED_DIR, "consolidado.csv")
    df_final.to_csv(output_path, index=False, encoding='utf-8', sep=';')
    
    print(f"Arquivo consolidado salvo em: {output_path}")
    print(f"Total de registros: {len(df_final)}")

if __name__ == "__main__":
    processar_arquivos()