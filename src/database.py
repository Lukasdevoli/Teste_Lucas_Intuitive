import sqlite3
import pandas as pd
import os

# Configurações
DB_PATH = "data/intuitive_care.db"
CSV_DESPESAS = "data/processed/consolidado_despesas_final.csv"
SQL_SCRIPT_PATH = "sql/querys.sql"

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def criar_tabelas():
    print(">>> Criando tabelas no banco de dados...")
    conn = get_connection()
    cursor = conn.cursor()
    
    # Verifica se o arquivo SQL existe antes de tentar abrir
    if not os.path.exists(SQL_SCRIPT_PATH):
        print(f"Arquivo SQL não encontrado em '{SQL_SCRIPT_PATH}'. Verifique o caminho e tente novamente.")
        conn.close()
        return
    
    try:
        with open(SQL_SCRIPT_PATH, 'r', encoding='utf-8') as f:
            script = f.read()
            
        partes = script.split('-- 3.4. QUERIES ANALÍTICAS')
        sql_ddl = partes[0]
        
        cursor.executescript(sql_ddl)
        conn.commit()
        print("Tabelas criadas com sucesso.")
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
    finally:
        conn.close()

def importar_dados():
    print(">>> Importando dados dos CSVs para o SQL...")
    
    if not os.path.exists(CSV_DESPESAS):
        print("CSV de despesas não encontrado. Rode o transformer.py.")
        return

    conn = get_connection()
    
    try:
        df = pd.read_csv(CSV_DESPESAS, sep=';', encoding='utf-8')
        
        df_ops = df[['RegistroANS', 'CNPJ', 'RazaoSocial', 'Modalidade', 'UF']].drop_duplicates(subset=['RegistroANS'])
        
        df_ops.columns = ['registro_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']
        
        
        conn.execute("DELETE FROM operadoras")
        df_ops.to_sql('operadoras', conn, if_exists='append', index=False)
        print(f"Importadas {len(df_ops)} operadoras.")
        
      
        df_desp = df[['RegistroANS', 'Ano', 'Trimestre', 'Valor Despesas']]
        df_desp.columns = ['registro_ans', 'ano', 'trimestre', 'valor_despesas']
        
        conn.execute("DELETE FROM despesas")
        df_desp.to_sql('despesas', conn, if_exists='append', index=False)
        print(f"Importados {len(df_desp)} registros de despesas.")
        
        conn.commit()
        
    except Exception as e:
        print(f"Erro na importação: {e}")
    finally:
        conn.close()

def executar_query_teste():
    print("\n>>> Testando banco de dados (Query: Top 3 Despesas):")
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT o.razao_social, d.valor_despesas 
        FROM despesas d 
        JOIN operadoras o ON d.registro_ans = o.registro_ans 
        ORDER BY d.valor_despesas DESC LIMIT 3
    """)
    
    for row in cursor.fetchall():
        print(f" - {row[0]}: R$ {row[1]:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    criar_tabelas()
    importar_dados()
    executar_query_teste()