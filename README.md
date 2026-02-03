# Teste Técnico – Intuitive Care

Este projeto implementa uma solução completa de **Engenharia de Dados** e **Desenvolvimento Web**, abrangendo desde a coleta de dados públicos da **ANS (Web Scraping)** até a visualização em um **Dashboard interativo**.

**Autor:** Lucas  
**Data:** Fevereiro/2026

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
- Python 3.8+
- Navegador Web moderno

---

## 1. Instalação das Dependências

No terminal, na raiz do projeto:

```bash
pip install flask flask-cors pandas requests beautifulsoup4 openpyxl
```

---

## 2. Execução do Pipeline de Dados em Ordem (ETL)

Execute os scripts **na ordem abaixo** para baixar, processar e carregar os dados.

### Coleta de Dados

```bash
python src/scraper.py
```

> Baixa as demonstrações contábeis dos últimos 3 trimestres disponíveis no FTP da ANS.

### Processamento Inicial

```bash
python src/processor.py
```

> Extrai os arquivos ZIP, localiza os CSVs de "Eventos/Sinistros" e normaliza as colunas gerando o `consolidado.csv`.

---

### Enriquecimento e Transformação

```bash
python src/transformer.py
```

> Baixa o cadastro de operadoras, realiza o cruzamento (JOIN) e valida os dados.

---

### Carga no Banco de Dados

```bash
python src/database.py
```

> Cria o banco SQLite e importa os dados processados.

---

## 3. Execução da Aplicação

### Inicie o servidor Backend:

```bash
python src/api/app.py
```

### Frontend

O frontend pode ser executado utilizando a extensão **Live Server** do VS Code.

1. Abra a pasta `src/frontend` no VS Code  
2. Clique com o botão direito no `index.html`  
3. Selecione **"Open with Live Server"**

A aplicação será aberta automaticamente no navegador.

---

## 🛠️ Decisões Técnicas e Trade-offs

Conforme solicitado no teste, abaixo estão as justificativas para as escolhas técnicas adotadas, priorizando **simplicidade e eficiência (Princípio KISS)**.

---

### 1️⃣ Web Scraping & ETL

**Trade-off:** Processamento em Memória (Pandas) vs Streaming  

**Decisão:** Uso de **Pandas com processamento em memória**

**Justificativa:**  
O volume de dados dos últimos 3 trimestres cabe confortavelmente na memória RAM de máquinas modernas. O Pandas acelera o desenvolvimento e oferece robustez no tratamento de dados, como:
- Encoding `latin1`
- Remoção de caracteres especiais  

Essas operações seriam mais complexas em um modelo de processamento linha a linha (streaming).

**Estratégia de Join e Dados Incompletos:**

**Desafio:** Inconsistência na formatação da chave RegistroANS (ex: `3456.0` vs `3456`) e indisponibilidade momentânea do site da ANS.

**Solução:** Implementação de limpeza matemática nas chaves antes do cruzamento e uso de Left Join. Registros sem correspondência no cadastro são marcados como "DESCONHECIDO" para preservar os valores contábeis totais, priorizando a integridade financeira sobre a completude cadastral.

---

### 2️⃣ Banco de Dados

**Trade-off:** SQLite vs MySQL/PostgreSQL  

**Decisão:** **SQLite**

**Justificativa:**  
- Elimina configuração de servidor, Docker ou credenciais  
- Banco em arquivo único (`.db`)  
- Portabilidade total para o avaliador  
- SQL padrão ANSI, permitindo migração fácil para PostgreSQL em produção  

---

### 3️⃣ Backend (API)

**Trade-off:** Flask vs FastAPI  

**Decisão:** **Flask**

**Justificativa:**  
Para um microsserviço **read-heavy**, o Flask oferece:
- Configuração mínima  
- Baixa curva de aprendizado  
- Simplicidade  

A paginação é feita via `LIMIT / OFFSET` diretamente no SQL, garantindo respostas leves em JSON para o frontend.

---

### 4️⃣ Frontend

**Trade-off:** Vue.js (CDN) vs Vue CLI / Vite  

**Decisão:** **Vue.js via CDN (HTML único)**

**Justificativa:**  
- Evita dependência de Node.js / NPM  
- Sem etapa de build  
- Basta abrir o arquivo HTML para funcionar  

O frontend é **serverless em termos de infraestrutura local**.

---

## 📄 Documentação da API
A API disponibiliza os seguintes endpoints:

- GET / - Status da API.
- GET /api/estatisticas - Retorna KPIs gerais (Total, Média, Top 5).
- GET /api/operadoras - Listagem paginada com busca textual.
- Params: page, limit, search.
- GET /api/operadoras/<id>/despesas - Histórico detalhado de despesas de uma operadora.

---

## 📂 Estrutura de Arquivos

```text
Teste_Lucas_Intuitive/
│
├── data/                      # Camada de Dados (Data Lakehouse Local)
│   ├── raw/                   # Arquivos brutos baixados da ANS (ZIPs, CSVs)
│   ├── processed/             # Dados tratados e consolidados
│   └── intuitive_care.db      # Banco de Dados SQLite
│
├── src/                       # Código Fonte
│   ├── api/
│   │   └── app.py             # Servidor Backend (Flask)
│   ├── frontend/
│   │   └── index.html         # Interface do Usuário (Vue.js + Bootstrap)
│   ├── scraper.py             # Coleta de dados (Web Scraping)
│   ├── processor.py           # Extração e Normalização (ETL - Fase 1)
│   ├── transformer.py         # Enriquecimento e Validação (ETL - Fase 2)
│   └── database.py            # Persistência e Modelagem (SQL)
│
├── sql/
│   └── queries.sql            # Scripts DDL e Queries Analíticas solicitadas
│
├── requirements.txt           # Dependências do Python
└── README.md                  # Documentação do Projeto
```
