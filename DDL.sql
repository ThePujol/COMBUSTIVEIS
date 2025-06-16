-- ======================================================
-- 1) Criação do banco de dados
-- ======================================================
DROP DATABASE IF EXISTS precos_combustiveis2;
CREATE DATABASE precos_combustiveis2
  CHARACTER SET latin1
  COLLATE latin1_swedish_ci;
USE precos_combustiveis2;

-- ======================================================
-- 2) Tabela de staging para bulk‐load dos CSVs
-- ======================================================
DROP TABLE IF EXISTS staging_precos;
CREATE TABLE staging_precos (
  regiao_sigla      VARCHAR(5),
  estado_sigla      VARCHAR(5),
  municipio         VARCHAR(100),
  revenda_nome      VARCHAR(150),
  revenda_cnpj      VARCHAR(20),
  rua               VARCHAR(150),
  numero            VARCHAR(20),
  complemento       VARCHAR(150),
  bairro            VARCHAR(100),
  cep               VARCHAR(15),
  produto_nome      VARCHAR(100),
  data_coleta_str   VARCHAR(10),
  valor_venda_str   VARCHAR(20),
  valor_compra_str  VARCHAR(20),
  unidade_medida    VARCHAR(20),
  bandeira          VARCHAR(100)
) ENGINE=InnoDB;

-- ======================================================
-- 3) Tabelas de dimensão
-- ======================================================
-- Região
CREATE TABLE regiao (
  id   INT AUTO_INCREMENT PRIMARY KEY,
  sigla VARCHAR(5) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- Estado
CREATE TABLE estado (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  sigla     VARCHAR(5) NOT NULL UNIQUE,
  regiao_id INT NOT NULL,
  FOREIGN KEY (regiao_id) REFERENCES regiao(id)
) ENGINE=InnoDB;

-- Município
CREATE TABLE municipio (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  nome       VARCHAR(100) NOT NULL,
  estado_id  INT NOT NULL,
  FOREIGN KEY (estado_id) REFERENCES estado(id),
  UNIQUE(nome, estado_id)
) ENGINE=InnoDB;

-- Endereço
CREATE TABLE endereco (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  rua          VARCHAR(150) NOT NULL,
  numero       VARCHAR(20),
  complemento  VARCHAR(150),
  bairro       VARCHAR(100),
  cep          VARCHAR(15),
  municipio_id INT NOT NULL,
  FOREIGN KEY (municipio_id) REFERENCES municipio(id),
  UNIQUE(rua, numero, complemento, bairro, cep, municipio_id)
) ENGINE=InnoDB;

-- Revenda
CREATE TABLE revenda (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  nome        VARCHAR(150) NOT NULL,
  cnpj        VARCHAR(20) NOT NULL UNIQUE,
  endereco_id INT NOT NULL,
  bandeira    VARCHAR(100),
  FOREIGN KEY (endereco_id) REFERENCES endereco(id)
) ENGINE=InnoDB;

-- Produto
CREATE TABLE produto (
  id             INT AUTO_INCREMENT PRIMARY KEY,
  nome           VARCHAR(100) NOT NULL,
  unidade_medida VARCHAR(20),
  UNIQUE(nome, unidade_medida)
) ENGINE=InnoDB;

-- ======================================================
-- 4) Tabela de fatos
-- ======================================================
CREATE TABLE preco (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  revenda_id    INT NOT NULL,
  produto_id    INT NOT NULL,
  data_coleta   DATE NOT NULL,
  valor_venda   DECIMAL(10,4),
  valor_compra  DECIMAL(10,4),
  FOREIGN KEY (revenda_id) REFERENCES revenda(id),
  FOREIGN KEY (produto_id) REFERENCES produto(id)
) ENGINE=InnoDB;
