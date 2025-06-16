# Preços de Combustíveis ETL

Este projeto automatiza a carga de dados de arquivos CSV (ANP) para um banco MySQL, utilizando uma área de **staging** e operações em *batch* para garantir desempenho otimizado.

---

## 📂 Estrutura de Arquivos

* `ddl_completo.sql`
  Script DDL completo que cria o banco `precos_combustiveis2`, a tabela de *staging* (`staging_precos`), as dimensões (`regiao`, `estado`, `municipio`, `endereco`, `revenda`, `produto`) e a tabela de fatos (`preco`).

* `etl_batch_anp.py`
  Script Python que:

  1. Limpa a tabela de *staging* (`TRUNCATE`).
  2. Carrega todos os CSVs da pasta via `LOAD DATA LOCAL INFILE` na tabela de *staging*.
  3. Popula as dimensões (`INSERT IGNORE … SELECT DISTINCT`).
  4. Insere em *batch* na tabela de fatos (`INSERT … SELECT`), aplicando conversão de data e normalização de strings.

## 🚀 Requisitos

* Python 3.8+
* Dependências Python:

  ```bash
  pip install mysql-connector-python
  ```
* Servidor MySQL com *local\_infile* ativado
* Privilegios no banco `precos_combustiveis2` para executar DDL e `LOAD DATA`

## ⚙️ Instalação e Uso

1. **Clonar o repositório**

   ```bash
   git clone <URL_DO_REPO>
   cd repositorio
   ```

2. **Criar o banco e tabelas**

   ```bash
   mysql -u <usuário> -p < ddl_completo.sql
   ```

3. **Ajustar credenciais no script**

   * No `etl_batch_anp.py`, edite o dicionário `DB_CONFIG` com seu usuário, senha e host

4. **Executar ETL**

   ```bash
   python etl_batch_anp.py
   ```

5. **Verificar resultados**

   ```sql
   USE precos_combustiveis2;
   SELECT COUNT(*) FROM preco;
   ```

## 📌 Observações

* Se houver erros de permissão em `local_infile`, habilite no MySQL:

  ```sql
  SET GLOBAL local_infile = 1;
  ```
* A tabela de *staging* é truncada a cada execução para evitar duplicação.
* O script assume que todos os CSVs utilizam `;` como delimitador e estão codificados em `latin1`.


