import requests
from bs4 import BeautifulSoup
import os

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

def listar_links(url):

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erro ao acessar {url}")
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    links = []

    for link in soup.find_all('a'):
        href = link.get('href')

        if href.endswith('/') and href != '../':
            links.append(href)

    return links


def baixar_arquivos_recentes(pasta_destino):

    print("Iniciando busca pelos arquivos...")

    anos = listar_links(BASE_URL)
    anos.sort(reverse=True)

    trimestres_encontrados = 0
    arquivos_baixados = []

    os.makedirs(pasta_destino, exist_ok= True)

    for ano in anos:
        url_ano = BASE_URL + ano
        trimestres = listar_links(url_ano)
        trimestres.sort(reverse=True)

        for tri in trimestres:
            if trimestres_encontrados >= 3:
                break

            print(f"Acessando {ano}{tri}...")
            url_tri = url_ano + tri

            resp_tri = requests.get(url_tri)
            soup_tri = BeautifulSoup(resp_tri.text, 'html.parser')

            for link in soup_tri.find_all('a'):
                href = link.get('href')
                if href.endswith('.zip'):
                    link_download = url_tri + href
                    caminho_salvar = os.path.join(pasta_destino, f"{ano.strip('/')}_{tri.strip('/')}.zip")

                    print(f"Baixando {href}...")
                    conteudo_zip = requests.get(link_download).content

                    with open(caminho_salvar, 'wb') as f:
                        f.write(conteudo_zip)

                    arquivos_baixados.append(caminho_salvar)
                    trimestres_encontrados += 1

        if trimestres_encontrados >= 3:
            break
    
    return arquivos_baixados

if __name__ == "__main__":
    baixar_arquivos_recentes("data/raw")