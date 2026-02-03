import pandas as pd
import requests
import os
import zipfile
import io
import re

# Para ignorar o aviso de segurança
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
FILE_CONSOLIDADO = "data/processed/consolidado.csv"
DIR_PROCESSED = "data/processed"

def validar_cnpj(cnpj):
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14: return False
    
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(a) * b for a, b in zip(cnpj[:12], pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1
    if int(cnpj[12]) != digito1: return False

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(a) * b for a, b in zip(cnpj[:13], pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2
    if int(cnpj[13]) != digito2: return False

    return True

def baixar_cadastro_simples():
    print(">>> Tentando baixar cadastro...")
    try:
        r = requests.get(URL_CADASTRO, verify=False, timeout=30)
        
        texto = r.text
        link_csv = None
        
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(texto, 'html.parser')
        
        for a in soup.find_all('a'):
            href = a.get('href')
            if href:
                if 'relatorio' in href.lower() or 'cadop' in href.lower():
                    if href.endswith('.csv') or href.endswith('.zip'):
                        link_csv = URL_CADASTRO + href
                        break
        
        if not link_csv:
            print("Não achei o link automaticamente.")
            return None
            
        print(f"Baixando de: {link_csv}")
        r_arquivo = requests.get(link_csv, verify=False)
        
        caminho_final = "data/raw/cadastro_operadoras.csv"
        
        if link_csv.endswith('.zip'):
            z = zipfile.ZipFile(io.BytesIO(r_arquivo.content))
            nome_csv = [n for n in z.namelist() if n.endswith('.csv')][0]
            with z.open(nome_csv) as f:
                with open(caminho_final, 'wb') as out:
                    out.write(f.read())
        else:
            with open(caminho_final, 'wb') as f:
                f.write(r_arquivo.content)
                
        return caminho_final

    except Exception as e:
        print(f"Erro no download: {e}")
        return None

def main():
    print(">>> Lendo consolidado...")
    if not os.path.exists(FILE_CONSOLIDADO):
        print("Arquivo consolidado não existe!")
        return
        
    df_despesas = pd.read_csv(FILE_CONSOLIDADO, sep=';', encoding='utf-8', dtype=str)

    path_cadastro = "data/raw/cadastro_operadoras.csv"
    if not os.path.exists(path_cadastro):
        path_cadastro = baixar_cadastro_simples()
    
    if not path_cadastro:
        print("Sem arquivo de cadastro. Parando.")
        return

    print(">>> Lendo cadastro...")
    try:
        df_cadastro = pd.read_csv(path_cadastro, sep=';', encoding='latin1', dtype=str)
    except:
        df_cadastro = pd.read_csv(path_cadastro, sep=';', encoding='utf-8', dtype=str)

    print(">>> Arrumando colunas...")
    df_cadastro.columns = [c.lower().strip() for c in df_cadastro.columns]
    
    mapa = {}
    
    for col in df_cadastro.columns:
        if col == 'registro_operadora':
            mapa[col] = 'RegistroANS'
        elif 'registro' in col and 'ans' in col and 'data' not in col: 
            mapa[col] = 'RegistroANS'
        elif 'cd_operadora' in col:
            mapa[col] = 'RegistroANS'
        elif 'cnpj' in col:
            mapa[col] = 'CNPJ'
        elif 'razao' in col:
            mapa[col] = 'RazaoSocial'
        elif 'fantasia' in col and 'RazaoSocial' not in mapa.values():
            mapa[col] = 'RazaoSocial'
        elif col == 'uf':
            mapa[col] = 'UF'
        # --- CORREÇÃO AQUI: Adicionado mapeamento da Modalidade ---
        elif 'modalidade' in col:
            mapa[col] = 'Modalidade'
            
    df_cadastro.rename(columns=mapa, inplace=True)
    
    if 'RegistroANS' not in df_cadastro.columns:
        print("Erro: Não achei a coluna RegistroANS no cadastro.")
        return

    print(">>> Padronizando chaves...")
    
    df_despesas['RegistroANS'] = pd.to_numeric(df_despesas['RegistroANS'], errors='coerce')
    df_cadastro['RegistroANS'] = pd.to_numeric(df_cadastro['RegistroANS'], errors='coerce')
    
    df_despesas = df_despesas.dropna(subset=['RegistroANS'])
    df_cadastro = df_cadastro.dropna(subset=['RegistroANS'])
    
    df_despesas['RegistroANS'] = df_despesas['RegistroANS'].astype(int).astype(str)
    df_cadastro['RegistroANS'] = df_cadastro['RegistroANS'].astype(int).astype(str)

    print(">>> Fazendo o Join...")
    cols_para_pegar = ['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']
    cols_existentes = [c for c in cols_para_pegar if c in df_cadastro.columns]
    
    if 'RazaoSocial' in df_despesas.columns:
        df_despesas = df_despesas.drop(columns=['RazaoSocial', 'CNPJ'])

    df_final = pd.merge(df_despesas, df_cadastro[cols_existentes], on='RegistroANS', how='left')
    
    df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna("DESCONHECIDO")
    df_final['UF'] = df_final['UF'].fillna("IND")
    # Previne erro se modalidade não vier
    if 'Modalidade' not in df_final.columns:
        df_final['Modalidade'] = 'Desconhecida'
    else:
        df_final['Modalidade'] = df_final['Modalidade'].fillna("Desconhecida")

    print(">>> Salvando arquivos...")
    
    if 'CNPJ' in df_final.columns:
        df_final['CNPJ_Limpo'] = df_final['CNPJ'].astype(str).str.replace(r'[^0-9]', '', regex=True)
        df_final['CNPJ_Valido'] = df_final['CNPJ_Limpo'].apply(validar_cnpj)
        df_final = df_final.drop(columns=['CNPJ_Limpo'])

    df_final.to_csv("data/processed/consolidado_despesas_final.csv", index=False, sep=';', encoding='utf-8')
    
    if 'Valor Despesas' in df_final.columns:
        df_final['Valor Despesas'] = pd.to_numeric(df_final['Valor Despesas'], errors='coerce').fillna(0)
        
        agregado = df_final.groupby(['RazaoSocial', 'UF'])['Valor Despesas'].sum().reset_index()
        agregado = agregado.sort_values(by='Valor Despesas', ascending=False)
        agregado.to_csv("data/processed/despesas_agregadas.csv", index=False, sep=';', encoding='utf-8')
        
    print("Concluído!")

if __name__ == "__main__":
    main()