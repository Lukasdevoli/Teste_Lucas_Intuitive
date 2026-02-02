from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.join(os.getcwd(), "data/intuitive_care_db")

@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "message": "API Intuitive Care rodando!",
        "endpoints": {
            "estatisticas": "/api/estatisticas",
            "operadoras": "/api/operadoras"
        }
    })

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/api/operadoras', methods=['GET'])

def list_operadoras():
    try:
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 10))
        search = request.args.get('search', '')

        offset = (page - 1) * limit

        conn = get_db_connection()

        query = "SELECT * FROM operadoras WHERE 1=1"
        params = []

        if search:
            query += "AND (razao_social LIKE ? OR cnpj LIKE?)"
            term = f"%{search}%"
            params.extend([term, term])

        
        count_query = f"SELECT COUNT(*) FROM ([{query}])"
        total = conn.execute(count_query, params).fetchone([0])

        query += "LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = conn.execute(query)
        rows = cursor.fetchall()
        conn.close()

        results = [dict(row) for row in rows]

        return jsonify({
            'data': results,
            'meta': {
                'page': page,
                'limit': limit,
                'total': total,
                'total_pages': (total + limit - 1)// limit
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    


# Rota 2 - Detalhes Operadora
@app.route('/api/operadoras/<cnpj_ou_registro>', methods=['GET'])

def get_operadora_details(cnpj_ou_registro):
    conn = get_db_connection()
    clean_id = cnpj_ou_registro.replace('.', '').replace('/', '').replace('-', '')

    operadora = conn.execute(
        "SELECT * FROM operadoras WHERE cnpj = ? OR registro_ans = ?",
        (clean_id, clean_id)
    ).fetchone()

    conn.close()

    if operadora is None:
        return jsonify({'error': 'Operadora não encontrada'}), 400
    
    return jsonify(dict(operadora))


#Rota 3 
@app.route('/api/operadoras/<registro_ans>/despesas', methods=['GET'])

def get_operadoras_despesas(registro_ans):
    conn = get_db_connection()
    rows = conn.execute(
        "SELECT ano, trimestre, valor_despesas FROM despesas WHERE registro_ans = ? ORDER BY ano DESC, trimestre DESC",
        (registro_ans,)
    ).fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

#Rota 4
@app.route('/api/estatisticas', methods=['GET'])
def get_estatisticas():
    
    conn = get_db_connection()
    
    total_despesas = conn.execute("SELECT SUM(valor_despesas) FROM despesas").fetchone()[0] or 0
    
    media = conn.execute("SELECT AVG(valor_despesas) FROM despesas").fetchone()[0] or 0
    
    top_5 = conn.execute("""
        SELECT o.razao_social, SUM(d.valor_despesas) as total
        FROM despesas d
        JOIN operadoras o ON d.registro_ans = o.registro_ans
        GROUP BY o.razao_social
        ORDER BY total DESC
        LIMIT 5
    """).fetchall()
    
    conn.close()

    return jsonify({
        'total_geral': total_despesas,
        'media_trimestral': media,
        'top_operadoras': [dict(row) for row in top_5]
    })

if __name__ == '__main__':
    
    print("Servidor API rodando em http://localhost:5000")
    app.run(debug=True, port=5000)
