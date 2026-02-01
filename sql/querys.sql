
-- ============================================================================
-- 3.2. DDL - CRIAÇÃO DAS TABELAS
-- ============================================================================

CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans VARCHAR(20) PRIMARY KEY,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    modalidade VARCHAR(100),
    uf VARCHAR(2)
);

CREATE TABLE IF NOT EXISTS despesas (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Identificador único da linha
    registro_ans VARCHAR(20),
    ano INTEGER,
    trimestre VARCHAR(10),
    valor_despesas DECIMAL(15, 2),
    descricao VARCHAR(255),
    FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_despesas_ans ON despesas(registro_ans);
CREATE INDEX IF NOT EXISTS idx_despesas_tempo ON despesas(ano, trimestre);

-- ============================================================================
-- 3.4. QUERIES ANALÍTICAS
-- ============================================================================

SELECT 
    o.razao_social, 
    d.valor_despesas 
FROM despesas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
WHERE d.ano = (SELECT MAX(ano) FROM despesas) 
  AND d.trimestre = (SELECT MAX(trimestre) FROM despesas WHERE ano = (SELECT MAX(ano) FROM despesas))
ORDER BY d.valor_despesas DESC
LIMIT 10;

-- Query 2: As 5 operadoras com maior média de despesas por ano
SELECT 
    o.razao_social,
    AVG(d.valor_despesas) as media_despesa
FROM despesas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
GROUP BY o.razao_social
ORDER BY media_despesa DESC
LIMIT 5;

-- Query 3: Total de despesas por UF (Último ano)
SELECT 
    o.uf,
    SUM(d.valor_despesas) as total_despesas
FROM despesas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
WHERE d.ano = (SELECT MAX(ano) FROM despesas)
GROUP BY o.uf
ORDER BY total_despesas DESC;