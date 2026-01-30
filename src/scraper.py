import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; scraper/1.0; +https://example.com)'}

def obter_trimestres_recentes(qtd=3, incluir_atual=False):
    
    hoje = datetime.now()
    trimestres = []

    def trimestre_de_data(ano, mes):
        q = (mes - 1) // 3 + 1
        return f"{ano}/Q{q}"

    start_index = 0 if incluir_atual else 1
    
    for i in range(start_index, qtd + start_index):
        mes = hoje.month - 3 * i
        ano = hoje.year
        while mes <= 0:
            mes += 12
            ano -= 1
        trimestres.append(trimestre_de_data(ano, mes))

    return trimestres

def listar_hrefs(url):

    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
    except requests.RequestException as e:
        print(f"Erro de conexão em {url}: {e}")
        return []
        
    if response.status_code != 200:
        print(f"Erro ao acessar {url}: status {response.status_code}")
        return []
        
    soup = BeautifulSoup(response.text, 'html.parser')
    hrefs = []
    for a in soup.find_all('a'):
        href = a.get('href')
        if href and href != '../':
            hrefs.append(href)
    return hrefs

def baixar_arquivos_recentes(pasta_destino):
    print("Iniciando processo de download...")
    trimestres = obter_trimestres_recentes()
    arquivos_baixados = []
    os.makedirs(pasta_destino, exist_ok=True)

    for trimestre in trimestres:
        try:
            parts = trimestre.split('/Q')
            if len(parts) != 2:
                continue
            
            ano, q = parts[0], parts[1]
            url_ano = f"{BASE_URL}{ano}/"
            
            print(f"Verificando {url_ano}...")
            hrefs = listar_hrefs(url_ano)
            
            # Padrão esperado: 1T2024.zip, 2T2023.zip, etc.
            target_filename = f"{q}T{ano}.zip"
            candidatos = [h for h in hrefs if h.endswith(target_filename)]
            
            if not candidatos:
                print(f"Arquivo {target_filename} não encontrado.")
                continue

            for filename in candidatos:
                link_download = url_ano + filename
                nome_final = f"{ano}_{q}T_{filename}"
                caminho_salvar = os.path.join(pasta_destino, nome_final)
                
                if os.path.exists(caminho_salvar):
                    print(f"Arquivo já existente: {nome_final}")
                    arquivos_baixados.append(caminho_salvar)
                    continue
                
                print(f"Baixando {filename}...")
                resp_file = requests.get(link_download, headers=HEADERS, timeout=30)
                
                if resp_file.status_code == 200:
                    with open(caminho_salvar, 'wb') as f:
                        f.write(resp_file.content)
                    arquivos_baixados.append(caminho_salvar)
                    print(f"Sucesso: {nome_final}")
                else:
                    print(f"Falha no download. Status: {resp_file.status_code}")

        except Exception as e:
            print(f"Erro ao processar trimestre {trimestre}: {e}")

    return arquivos_baixados

if __name__ == "__main__":
    baixar_arquivos_recentes("data/raw")