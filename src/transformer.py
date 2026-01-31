import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import io
import re

# Configurações
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
FILE_CONSOLIDADO = "data/processed/consolidado.csv"
DIR_PROCESSED = "data/processed"
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; transformer/1.0)'}
REQUEST_TIMEOUT = 10

def validar_cnpj(cnpj):
    
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    # Validação do primeiro dígito
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(a) * b for a, b in zip(cnpj[:12], pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    if int(cnpj[12]) != digito1:
        return False

    # Validação do segundo dígito
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(a) * b for a, b in zip(cnpj[:13], pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    if int(cnpj[13]) != digito2:
        return False

    return True

def baixar_cadastro():
    print(">>> Baixando Cadastro de Operadoras...")
    try:
        resp = requests.get(URL_CADASTRO, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        print(f"Erro ao conectar em {URL_CADASTRO}: {e}")
        return None

    if resp.status_code != 200:
        print(f"Erro ao acessar {URL_CADASTRO}: status {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')
    
    link_csv = None
    for link in soup.find_all('a'):
        href = link.get('href')
        
        if href and ('Relatorio_Cadop' in href) and (href.endswith('.csv') or href.endswith('.zip')):
            link_csv = URL_CADASTRO + href
            break
    
    if not link_csv:
        print("Link do cadastro não encontrado. Usando arquivo local se existir.")
        return None

    os.makedirs("data/raw", exist_ok=True)
    file_name = os.path.join("data/raw", "cadastro_operadoras.csv")

    # Download
    try:
        r = requests.get(link_csv, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        print(f"Erro ao baixar {link_csv}: {e}")
        return None

    if r.status_code != 200:
        print(f"Falha ao baixar {link_csv}: status {r.status_code}")
        return None

    # Extração se o zip existir
    if link_csv.endswith('.zip'):
        try:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                csv_list = [n for n in z.namelist() if n.lower().endswith('.csv')]
                if not csv_list:
                    print(f"Zip baixado de {link_csv} não contém CSV.")
                    return None
                csv_name = csv_list[0]
                with z.open(csv_name) as f:
                    with open(file_name, 'wb') as out:
                        out.write(f.read())
        except zipfile.BadZipFile as e:
            print(f"Arquivo zip inválido: {e}")
            return None
    else:
        # Se for CSV direto
        with open(file_name, 'wb') as f:
            f.write(r.content)
            
    print("Cadastro baixado com sucesso.")
    return file_name

def main():
    if not os.path.exists(FILE_CONSOLIDADO):
        print("Erro: 'consolidado.csv' não encontrado. Rode o processor.py antes.")
        return

    print(">>> Carregando dados...")
    df_despesas = pd.read_csv(FILE_CONSOLIDADO, sep=';', encoding='utf-8')
    
    df_despesas['RegistroANS'] = df_despesas['RegistroANS'].astype(str).str.replace(r'\.0$', '', regex=True)

    caminho_cadastro = "data/raw/cadastro_operadoras.csv"
    if not os.path.exists(caminho_cadastro):
        caminho_cadastro = baixar_cadastro()

    if not caminho_cadastro or not os.path.exists(caminho_cadastro):
        print("Cadastro não disponível (nenhum arquivo local e download falhou). Abortando transformação.")
        return

    try:
        df_cadastro = pd.read_csv(caminho_cadastro, sep=';', encoding='latin1', on_bad_lines='skip')
    except Exception as e:
        print(f"Erro ao ler cadastro em {caminho_cadastro}: {e}")
        return

    # renomeia se existirem colunas esperadas
    rename_map = {}
    if 'Registro_ANS' in df_cadastro.columns:
        rename_map['Registro_ANS'] = 'RegistroANS'
    if 'CNPJ' in df_cadastro.columns:
        rename_map['CNPJ'] = 'CNPJ'
    if 'Razao_Social' in df_cadastro.columns:
        rename_map['Razao_Social'] = 'RazaoSocial'
    if 'Modalidade' in df_cadastro.columns:
        rename_map['Modalidade'] = 'Modalidade'
    if 'UF' in df_cadastro.columns:
        rename_map['UF'] = 'UF'

    if rename_map:
        df_cadastro.rename(columns=rename_map, inplace=True)

    df_cadastro['RegistroANS'] = df_cadastro.get('RegistroANS', '').astype(str).str.replace(r'\.0$', '', regex=True)
    
  
    print(">>> Realizando Cruzamento (Join) de dados...")
    

    cols_drop = [c for c in ['CNPJ', 'RazaoSocial'] if c in df_despesas.columns]
    df_despesas.drop(columns=cols_drop, inplace=True)
    
    df_final = pd.merge(df_despesas, df_cadastro[['RegistroANS', 'CNPJ', 'RazaoSocial', 'UF', 'Modalidade']], 
                        on='RegistroANS', how='left')

    sem_match = df_final['RazaoSocial'].isnull().sum()
    if sem_match > 0:
        print(f"Aviso: {sem_match} registros de despesas não encontraram operadora no cadastro.")
        df_final['RazaoSocial'].fillna("DESCONHECIDO", inplace=True)
        df_final['UF'].fillna("IND", inplace=True) # Indefinido

    print(">>> Validando dados...")

    df_final['CNPJ'] = df_final['CNPJ'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    
    df_final['CNPJ_Valido'] = df_final['CNPJ'].apply(validar_cnpj)
    
    invalidos = (~df_final['CNPJ_Valido']).sum()
    print(f"CNPJs Inválidos encontrados: {invalidos}")
    
    path_consolidado = os.path.join(DIR_PROCESSED, "consolidado_despesas_final.csv")
    df_final.to_csv(path_consolidado, index=False, sep=';', encoding='utf-8')
    print(f"Consolidado Final salvo em: {path_consolidado}")

    print(">>> Gerando Agregações...")
    
    agregado = df_final.groupby(['RazaoSocial', 'UF']).agg({
        'Valor Despesas': ['sum', 'mean', 'std', 'count']
    }).reset_index()
    
    agregado.columns = ['RazaoSocial', 'UF', 'Despesa_Total', 'Despesa_Media_Trimestral', 'Desvio_Padrao', 'Qtd_Registros']
    
    agregado = agregado.sort_values(by='Despesa_Total', ascending=False)
    
    path_agregado = os.path.join(DIR_PROCESSED, "despesas_agregadas.csv")
    agregado.to_csv(path_agregado, index=False, sep=';', encoding='utf-8')
    print(f"Agregado salvo em: {path_agregado}")
    
    print("\n Processo de Transformação Concluído!")

if __name__ == "__main__":
    main()