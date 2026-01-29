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


