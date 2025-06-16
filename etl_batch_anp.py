#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import mysql.connector

# — ajuste credenciais e habilite LOCAL INFILE
DB_CONFIG = {
    'user': 'root',
    'password': 'tads',
    'host': 'localhost',
    'database': 'precos_combustiveis2',
    'allow_local_infile': True
}

def main():
    cnx = mysql.connector.connect(**DB_CONFIG)
    cursor = cnx.cursor()

    # Ativa local_infile (precisa de privilégios)
    cnx.commit()

    # 1) limpa staging
    cursor.execute("TRUNCATE TABLE staging_precos;")

    # 2) carrega cada CSV em bulk
    for f in glob.glob("*.csv"):
        print("→ carregando", f)
        sql = f"""
        LOAD DATA LOCAL INFILE '{f.replace("\\\\", "/")}'
        INTO TABLE staging_precos
        CHARACTER SET latin1
        FIELDS TERMINATED BY ';'
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES
        (regiao_sigla
        ,estado_sigla
        ,municipio
        ,revenda_nome        -- 3ª coluna do CSV
        ,revenda_cnpj        -- 4ª coluna
        ,rua                 -- 5ª coluna
        ,numero              -- 6ª coluna
        ,complemento         -- 7ª coluna
        ,bairro              -- 8ª coluna
        ,cep                 -- 9ª coluna
        ,produto_nome        -- 10ª coluna
        ,data_coleta_str     -- 11ª coluna
        ,valor_venda_str     -- 12ª coluna
        ,valor_compra_str    -- 13ª coluna
        ,unidade_medida      -- 14ª coluna
        ,bandeira            -- 15ª coluna
        )
        """
        cursor.execute(sql)
    cnx.commit()

    # 3) insere dimensões com INSERT…SELECT DISTINCT…IGNORE
    merges = [

    # Região
    """
    INSERT IGNORE INTO regiao(sigla)
    SELECT DISTINCT regiao_sigla
      FROM staging_precos
     WHERE regiao_sigla <> 'Regiao - Sigla';
    """,

    # Estado (já vincula regiao_id)
    """
    INSERT IGNORE INTO estado(sigla, regiao_id)
    SELECT DISTINCT s.estado_sigla, r.id
      FROM staging_precos s
      JOIN regiao r ON r.sigla = s.regiao_sigla
     WHERE s.estado_sigla <> 'Estado - Sigla';
    """,

    # Município
    """
    INSERT IGNORE INTO municipio(nome, estado_id)
    SELECT DISTINCT s.municipio, e.id
      FROM staging_precos s
      JOIN estado e ON e.sigla = s.estado_sigla;
    """,

    # Endereço
    """
    INSERT IGNORE INTO endereco(rua,numero,complemento,bairro,cep,municipio_id)
    SELECT DISTINCT s.rua, s.numero,
                    NULLIF(s.complemento,''), NULLIF(s.bairro,''), NULLIF(s.cep,''),
                    m.id
      FROM staging_precos s
      JOIN municipio m ON m.nome = s.municipio
                      AND m.estado_id = (
                         SELECT id FROM estado WHERE sigla=s.estado_sigla
                       );
    """,

    # Revenda
    """
    INSERT IGNORE INTO revenda(nome, cnpj, endereco_id, bandeira)
    SELECT DISTINCT s.revenda_nome, s.revenda_cnpj,
                    ed.id, NULLIF(s.bandeira,'')
      FROM staging_precos s
      JOIN endereco ed ON ed.rua = s.rua
                      AND ed.numero = s.numero
                      AND ed.municipio_id = (
                         SELECT id FROM municipio WHERE nome=s.municipio
                       );
    """,

    # Produto
    """
    INSERT IGNORE INTO produto(nome, unidade_medida)
    SELECT DISTINCT s.produto_nome, 
           TRIM(SUBSTRING_INDEX(s.unidade_medida,'/',-1))
      FROM staging_precos s;
    """
    ]

    for q in merges:
        print("→ merge:", q.split()[1])
        cursor.execute(q)
    cnx.commit()

    # 4) insere fatos em bulk via SELECT
    print("→ inserindo preços na tabela fato")
    cursor.execute("""
    INSERT INTO preco (revenda_id, produto_id, data_coleta, valor_venda, valor_compra)
    SELECT
      rv.id,
      pr.id,
      STR_TO_DATE(s.data_coleta_str, '%d/%m/%Y'),
      NULLIF(REPLACE(s.valor_venda_str, ',', '.'), ''),
      NULLIF(REPLACE(s.valor_compra_str, ',', '.'), '')
    FROM staging_precos s
    JOIN revenda rv
      ON rv.cnpj = s.revenda_cnpj
    JOIN produto pr
      ON pr.nome = s.produto_nome
     AND pr.unidade_medida = TRIM(SUBSTRING_INDEX(s.unidade_medida, '/', -1))
    WHERE
      TRIM(s.data_coleta_str) RLIKE '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
    """)
    cnx.commit()

    cursor.close()
    cnx.close()
    print("→ todos carregados em batch com sucesso!")
  
if __name__ == "__main__":
    main()
# This script is designed to perform an ETL (Extract, Transform, Load) process for a batch of CSV files containing fuel price data.
# It connects to a MySQL database, loads the data from CSV files into a staging table, and then processes the data to insert it into the appropriate dimension and fact tables.