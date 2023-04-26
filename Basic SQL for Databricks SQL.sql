-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # SQL Básico para Databricks
-- MAGIC O conteúdo deste notebook faz parte do curso **Data Analyst Learning Plan** fornecido pela Databricks Academy. O objetivo foi replicar os conceitos, comandos com a finalidade de verificar os comportamentos de comandos do SQL no Databricks. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação de um banco de dados
-- MAGIC O Primeiro passo é criar um banco de dados, para realizar o treinamento. Para isso, é verificado se o banco não existe e caso essa verificação seja verdadeira, o banco de dados é criado. 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS dbacademy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Utilizar o banco de dados criado
-- MAGIC Após a criação do banco de dados, é necessário utilizá-lo para que todos os comandos de manipulação de dados sejam utilizados no local correto. 
-- MAGIC 
-- MAGIC Os diferentes tipos de instruções (Transact-SQL) são categorizadas em: 
-- MAGIC 
-- MAGIC - Data Manipulation Language (DML)
-- MAGIC - Data Definition Language (DDL)
-- MAGIC - Data Control Language (DCL)
-- MAGIC - Transactional Control Language (TCL)
-- MAGIC 
-- MAGIC Para mais detalhes, recomendo a leitura deste link: https://www.thomazrossito.com.br/comandos-dml-ddl-dcl-tcl-sql-server/
-- MAGIC  

-- COMMAND ----------

USE dbacademy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criando tabelas no database proposto
-- MAGIC 
-- MAGIC Após criar o database, usar o database, o próximo passo é verificar se as tabelas já existem no banco de dados, e, caso existam, é necessário removê-las. 
-- MAGIC 
-- MAGIC As instruções abaixo retratam toda a verificação e remoção das tabelas.

-- COMMAND ----------

DROP TABLE IF EXISTS basic_sql_for_databricks_sql_customers_csv;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_customers;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_loyalty_segments;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_loyalty_segments_csv;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_sales_gold;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_silver_promo_prices;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_silver_purchase_orders;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_silver_sales_orders;
DROP TABLE IF EXISTS basic_sql_for_databricks_sql_source_silver_suppliers;
DROP TABLE IF EXISTS intro_to_databricks_sql_gym_logs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criação das tabelas
-- MAGIC Conforme o bloco anterior, todas as tabelas foram removidas, para que então seja possível a sua criação utilizando algumas fontes de dados disponibilizadas para realização do curso. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criação da tabela intro_to_databricks_sql_gym_logs
-- MAGIC 
-- MAGIC Os dados desta tabela estão armazenados em um repositório remoto, no formato JSON. 
-- MAGIC Para criar essa tabela em formato tabular, é utilizando juntamente do comando CREATE TABLE a instrução USING juntamente com o formato do arquivo, que neste caso é um JSON. Por fim, é especificado o diretório no qual os arquivos estão armazenados através do comando LOCATION. 
-- MAGIC 
-- MAGIC Após criada, verificamos os dados utilizando a instrução mais popular do SQL, o SELECT *

-- COMMAND ----------

--Cria a tabela
CREATE TABLE intro_to_databricks_sql_gym_logs 
    USING JSON
    LOCATION 'wasbs://courseware@dbacademy.blob.core.windows.net/introduction-to-databricks-sql/v01/gym-logs';

--Verifica os dados armazenados
SELECT * FROM intro_to_databricks_sql_gym_logs;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criação da tabela basic_sql_for_databricks_sql_customers_csv
-- MAGIC 
-- MAGIC Similar ao exemplo anterior, será criada agora uma tabela porém com a origem dos dados em formato CSV. A instrução USING também é utilizada, porém com o formato csv. Além disso, a instrução OPTIONS é adotado neste exemplo. Nessa instrução torna-se necessário informar o caminho (path) em que o arquivo está localizado, juntamente com o header com valor true que indica que a primeira linha de cada coluna é o cabeçalho do arquivo. Caso o valor seja falso, a primeira linha seria ignorada. Por fim a opção inferSchema com valor true verifica os valores e seta um determinado tipo de dado para cada coluna (INT, FLOAT, STRING, etc).

-- COMMAND ----------

CREATE TABLE basic_sql_for_databricks_sql_customers_csv
  USING csv 
  OPTIONS (
    path "wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/customers",
    header "true",
    inferSchema "true"
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criação da tabela basic_sql_for_databricks_sql_customers
-- MAGIC 
-- MAGIC Uma outra possibilidade de criação de tabelas é criar uma tabela através de uma tabela já existente. Neste caso, a tabela basic_sql_for_databricks_sql_customers_csv foi utilizada para gerar a tabela basic_sql_for_databricks_sql_customers e posteriormente a tabela basic_sql_for_databricks_sql_customers_csv foi removida.

-- COMMAND ----------

CREATE TABLE basic_sql_for_databricks_sql_customers AS
  SELECT * FROM basic_sql_for_databricks_sql_customers_csv;
DROP TABLE basic_sql_for_databricks_sql_customers_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criação das tabelas basic_sql_for_databricks_sql_loyalty_segments_csv e basic_sql_for_databricks_sql_loyalty_segments
-- MAGIC 
-- MAGIC Similar aos exemplos anteriores, mais algumas tabelas são criadas para dar continuidade com o conteúdo do curso.

-- COMMAND ----------

CREATE TABLE basic_sql_for_databricks_sql_loyalty_segments_csv
  USING csv 
  OPTIONS (
    path "wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/loyalty_segments",
    header "true",
    inferSchema "true"
);

-- COMMAND ----------

CREATE TABLE basic_sql_for_databricks_sql_loyalty_segments AS
  SELECT * FROM basic_sql_for_databricks_sql_loyalty_segments_csv;
DROP TABLE basic_sql_for_databricks_sql_loyalty_segments_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Criação de tabelas diversas
-- MAGIC 
-- MAGIC Por fim como última, algumas tabelas são criadas para dar continuidade ao curso. 

-- COMMAND ----------

CREATE TABLE basic_sql_for_databricks_sql_sales_gold AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/gold/sales`;
  
CREATE TABLE basic_sql_for_databricks_sql_silver_promo_prices AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/silver/promo_prices`;
  
CREATE TABLE basic_sql_for_databricks_sql_silver_purchase_orders AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/silver/purchase_orders.delta`;
  
CREATE TABLE basic_sql_for_databricks_sql_silver_sales_orders AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/silver/sales_orders`;
  
CREATE TABLE basic_sql_for_databricks_sql_silver_suppliers AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/silver/suppliers`;
  
CREATE TABLE basic_sql_for_databricks_sql_source_silver_suppliers AS
  SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/basic-sql-for-databricks-sql/v01/retail-org/solutions/silver/suppliers`;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recuperação dos dados
-- MAGIC 
-- MAGIC Após criar as tabelas, podemos visualizar os dados presentes nas mesmas, para isso é necessário utilizar o comando SELECT e suas diversas possibilidades. A seguir serão vistas algumas dessas possibilidades. 

-- COMMAND ----------

-- Selecionando todas as colunas, através do *
 SELECT * FROM dbacademy.basic_sql_for_databricks_sql_customers;

-- COMMAND ----------

-- Selecionando uma coluna específica
 SELECT customer_name AS Customer FROM dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- Selecionando uma coluna de maneira distinta, isto é, retorna os valores de maneira unificada
 SELECT DISTINCT state FROM dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- Selecionando um conjunto de colunas utilizando o comando WHERE. Desse modo, somente um conjunto de dados que respeita uma determinada regra será exibido.
 SELECT * FROM dbacademy.basic_sql_for_databricks_sql_customers WHERE loyalty_segment = 3;


-- COMMAND ----------

-- Utilizando o comando GROUP BY para agrupar valores
-- Essa consulta retornará um erro visto que o comando não foi utilizado da menira correta
 SELECT 
   * 
 FROM dbacademy.basic_sql_for_databricks_sql_customers 
   GROUP BY loyalty_segment;


-- COMMAND ----------

-- Utilizando o GROUP BY com a função de agregação COUNT. 
 SELECT   loyalty_segment
         ,count(loyalty_segment) 
         
from dbacademy.basic_sql_for_databricks_sql_customers 
GROUP BY loyalty_segment;


-- COMMAND ----------

-- Utilizando o ORDER BY para ordenação de valores
 SELECT 
         loyalty_segment, 
         count(loyalty_segment) 
         
from dbacademy.basic_sql_for_databricks_sql_customers 

GROUP BY loyalty_segment 
ORDER BY loyalty_segment;


-- COMMAND ----------

-- Utilizando a instrução HAVING juntamente com uma função de agregação COUNT
 SELECT 
       loyalty_segment, 
       count(loyalty_segment) AS loyalty_count 
       
from dbacademy.basic_sql_for_databricks_sql_customers 

GROUP BY loyalty_segment 
HAVING loyalty_count > 4000 
ORDER BY loyalty_segment;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Seleção de Delta Tables
-- MAGIC 
-- MAGIC A partir deste momento, uma propriedade específica presente no databricks é o Delta Lake. 
-- MAGIC 
-- MAGIC O Delta Lake é uma ambiente de armazenamento de software que tem como objetivo aumentar a confiabilidade para os data lakes. Algumas de suas característicia são o fornecimento de transações ACID, tratamento de metadados escalonáveis e unificação do processamento de dados de lote e streaming. Além disso, observa-se as Delta Tables que tem algumas funcionalidades muito interessantes tais como a possibilidade de visualizar um histórico de suas modificações bem como seu versionamento. Como o foco deste notebook não é a parte conceitual, recomendo uma busca pelo curso **Introduction to Delta Lake** presente no Databricks Academy.

-- COMMAND ----------

-- Seleção de Delta Tables
 DESCRIBE HISTORY dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Atualizando a tabela e visualizando o mecanimos de histórico e versionamento
-- MAGIC 
-- MAGIC A seguir é utilizando o comando UPDATE para realizar atualizações nos dados de acordo um critério especificado na cláusula WHERE. Por fim, pode-se observar o histórico criado, bem como o versionamento da tabela. 

-- COMMAND ----------

UPDATE dbacademy.basic_sql_for_databricks_sql_customers SET loyalty_segment = 10 WHERE loyalty_segment = 0;
DESCRIBE HISTORY dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

UPDATE dbacademy.basic_sql_for_databricks_sql_customers SET loyalty_segment = 0 WHERE loyalty_segment = 10;
DESCRIBE HISTORY dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Acessando uma versão da tabela através do TIMESTAMP AS OF
-- MAGIC 
-- MAGIC Com os recursos de versionamento e histórico temporal, uma das maneiras mais simplificadas de visualizar os dados é através do TIMESTAMP AS OF, capaz de resgatar uma determinada alteração realizada no passado. 

-- COMMAND ----------

SELECT loyalty_segment FROM dbacademy.basic_sql_for_databricks_sql_customers TIMESTAMP AS OF '2022-01-12T17:29:15.000Z';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Acessando uma versão da tabela através do VERSION AS OF
-- MAGIC 
-- MAGIC Outro modo de visualizar as alterações é utilizando o comando VERSION AS OF que buscará os dados conforme a versão registrada no histórico da tabela. 

-- COMMAND ----------

SELECT loyalty_segment FROM dbacademy.basic_sql_for_databricks_sql_customers VERSION AS OF 1;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Expressões colunares
-- MAGIC 
-- MAGIC A seguir serão utilizadas expressões colunares tais como multiplicação, subtração, manipulação de strings, dentre outras.

-- COMMAND ----------

-- Subtração e multiplicação
SELECT 
        sales_price - sales_price * promo_disc AS Calculated_Discount, 
        discounted_price AS Discounted_Price 
        
FROM dbacademy.basic_sql_for_databricks_sql_silver_promo_prices;


-- COMMAND ----------

-- Transformando todas as letras em minúsculas 
SELECT lower(city) AS City FROM dbacademy.basic_sql_for_databricks_sql_customers;

-- COMMAND ----------

-- Colocando a primeira letra em maiúscula
SELECT initcap(lower(city)) AS City FROM dbacademy.basic_sql_for_databricks_sql_customers;

-- COMMAND ----------

-- Função de data, convertendo timestamp
SELECT  
         promo_began 
        ,from_unixtime(promo_began)  
        
FROM dbacademy.basic_sql_for_databricks_sql_silver_promo_prices;


-- COMMAND ----------

-- Padrões de data e hora: https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-datetime-pattern.html
SELECT from_unixtime(promo_began, "d MMM, y") AS Beginning_Date FROM dbacademy.basic_sql_for_databricks_sql_silver_promo_prices;

-- COMMAND ----------

-- Cálculo de datas (neste caso uma subtração de duas datas)
SELECT current_date() - to_date(from_unixtime(promo_began)) FROM dbacademy.basic_sql_for_databricks_sql_silver_promo_prices;


-- COMMAND ----------

-- Utilização de CASE WHEN para criação de colunas a partir de validação de dados
SELECT customer_name, loyalty_segment,
  CASE 
     WHEN loyalty_segment = 0 THEN 'Rare'
     WHEN loyalty_segment = 1 THEN 'Occasional'
     WHEN loyalty_segment = 2 THEN 'Frequent'
     WHEN loyalty_segment = 3 THEN 'Daily'
  END AS Loyalty 
  FROM dbacademy.basic_sql_for_databricks_sql_customers;

-- COMMAND ----------

-- Utilização de CASE WHEN dentro de um ORDER BY
SELECT 
      * 
FROM dbacademy.basic_sql_for_databricks_sql_customers 
WHERE state = 'UT'
ORDER BY
 (CASE 
     WHEN city IS NULL THEN state
     ELSE city
 END);

-- COMMAND ----------

-- Manipulando dados e atualizando seus valores na tabela com o UPDATE. 
-- Visualizando os dados no seu formato original
 SELECT city FROM dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- Visualizando os dados tratados pré-update
SELECT initcap(lower(city)) AS City FROM dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- Realizando o update e visualizando os dados
UPDATE dbacademy.basic_sql_for_databricks_sql_customers SET city = initcap(lower(city));
SELECT city FROM dbacademy.basic_sql_for_databricks_sql_customers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Inserção de dados
-- MAGIC 
-- MAGIC Serão abordados exemplos utilizando os inserts tradicionais, já vistos em banco de dados relacionais, bem como o INSERT OVERWRITE e MERGE com possibilidade de inserção e atualização. 

-- COMMAND ----------

-- Inserção de dados na tabela
-- Visualização dos dados
SELECT * FROM dbacademy.basic_sql_for_databricks_sql_loyalty_segments;


-- COMMAND ----------

-- Inserção de dados com o comando INSERT INTO
INSERT INTO dbacademy.basic_sql_for_databricks_sql_loyalty_segments 
(loyalty_segment_id, loyalty_segment_description, unit_threshold, valid_from, valid_to)
VALUES
(4, 'level_4', 100, current_date(), Null);

-- COMMAND ----------

-- Inserção de dados vindo de uma tabela
-- Visualização dos dados
SELECT * FROM dbacademy.basic_sql_for_databricks_sql_silver_suppliers where password_hash = 'f6899b07c3868a5975438ee0caea6623';



-- COMMAND ----------

-- Inserção dos dados através do INSERT INTO TABLE e visualização dos dados
INSERT INTO dbacademy.basic_sql_for_databricks_sql_silver_suppliers TABLE dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers; 
SELECT * FROM dbacademy.basic_sql_for_databricks_sql_silver_suppliers where password_hash = 'f6899b07c3868a5975438ee0caea6623';

-- COMMAND ----------

-- Inserção de dados com sobrescrita
-- Visualização dos dados
select count(*) from  dbacademy.basic_sql_for_databricks_sql_silver_suppliers;



-- COMMAND ----------

-- Inserção dos dados utilizando o comando INSERT OVERWRITE
INSERT OVERWRITE dbacademy.basic_sql_for_databricks_sql_silver_suppliers TABLE dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers; 
select count(*) from  dbacademy.basic_sql_for_databricks_sql_silver_suppliers;

-- COMMAND ----------

-- Realização de merge de dados, isto é, mesclando os dados de dois conjuntos diferentes
-- MERGE NOT MATCHED THEN INSERT
MERGE INTO dbacademy.basic_sql_for_databricks_sql_silver_suppliers
   USING dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers
   ON dbacademy.basic_sql_for_databricks_sql_silver_suppliers.EAN13 = dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers.EAN13
   WHEN NOT MATCHED THEN INSERT *


-- COMMAND ----------

 -- MERGE MATCHED THEN UPDATE
 UPDATE dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers SET EAN13 = EAN13 + 1 WHERE EAN13 = 2198122549911;
 MERGE INTO dbacademy.basic_sql_for_databricks_sql_silver_suppliers
   USING dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers
   ON dbacademy.basic_sql_for_databricks_sql_silver_suppliers.EAN13 = dbacademy.basic_sql_for_databricks_sql_source_silver_suppliers.EAN13
   WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Subqueries
-- MAGIC 
-- MAGIC Serão visualizadas a seguir as possibilidades de utilização de subqueries. 

-- COMMAND ----------

-- Criando uma tabela utilizando subquery
 CREATE TABLE dbacademy.high_loyalty_customers AS
     SELECT * FROM dbacademy.basic_sql_for_databricks_sql_customers WHERE loyalty_segment = 3;



-- COMMAND ----------

-- Dropando a tabela
DROP TABLE dbacademy.high_loyalty_customers;



-- COMMAND ----------

-- Criando uma view utilizando subquery
 CREATE VIEW dbacademy.high_loyalty_customers AS
     SELECT * FROM dbacademy.basic_sql_for_databricks_sql_customers WHERE loyalty_segment = 3;
    


-- COMMAND ----------

-- Dropando a view
DROP VIEW dbacademy.high_loyalty_customers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Trabalhando com JOINs 
-- MAGIC 
-- MAGIC Um dos temas mais comuns no dia a dia de uma analista de dados, analista de banco de dados, analista de bi e engenheiro de dados é a manipulação de tabelas utilizando os joins, sendo eles o INNER, LEFT, RIGHT, FULL, CROSS, dentre outros. 

-- COMMAND ----------

-- INNER JOIN
 SELECT
   customer_name,
   loyalty_segment_description,
   unit_threshold
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   INNER JOIN dbacademy.basic_sql_for_databricks_sql_loyalty_segments
     ON basic_sql_for_databricks_sql_customers.loyalty_segment = basic_sql_for_databricks_sql_loyalty_segments.loyalty_segment_id;


-- COMMAND ----------

-- LEFT [OUTER] JOIN
 SELECT
   basic_sql_for_databricks_sql_customers.customer_name,
   product_category,
   total_price
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   LEFT JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold 
     ON basic_sql_for_databricks_sql_customers.customer_id = basic_sql_for_databricks_sql_sales_gold.customer_id
 WHERE
   state = 'NC'
 ORDER BY product_category DESC;

-- COMMAND ----------

-- RIGHT [OUTER] JOIN
SELECT
   region,
   product_category,
   total_price
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   RIGHT JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold 
     ON basic_sql_for_databricks_sql_customers.customer_id = basic_sql_for_databricks_sql_sales_gold.customer_id
 WHERE
     product_category = 'Sioneer'
 ORDER BY product_category DESC;

-- COMMAND ----------

-- FULL [OUTER] JOIN
 SELECT
   region,
   product_category,
   total_price
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   FULL JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold 
     ON basic_sql_for_databricks_sql_customers.customer_id = basic_sql_for_databricks_sql_sales_gold.customer_id
 ORDER BY product_category DESC;

-- COMMAND ----------

-- LEFT [SEMI] JOIN
 SELECT
   *
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   LEFT SEMI JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold 
     ON basic_sql_for_databricks_sql_customers.customer_id = basic_sql_for_databricks_sql_sales_gold.customer_id
 WHERE
   state = 'NC';

-- COMMAND ----------

-- LEFT [ANTI] JOIN
 SELECT
   *
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   LEFT ANTI JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold 
     ON basic_sql_for_databricks_sql_customers.customer_id = basic_sql_for_databricks_sql_sales_gold.customer_id
 WHERE
   state = 'NC';

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### CROSS JOIN

-- COMMAND ----------

SELECT
   count(*)
 FROM
   dbacademy.basic_sql_for_databricks_sql_sales_gold;

-- COMMAND ----------

 SELECT
   count(*)
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers

-- COMMAND ----------

 SELECT
   count(*)
 FROM
   dbacademy.basic_sql_for_databricks_sql_customers
   CROSS JOIN dbacademy.basic_sql_for_databricks_sql_sales_gold;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Funções de agregação
-- MAGIC 
-- MAGIC Embora algumas funções já foram vistas anteriormente, agora em detalhes as funções capazes de contar, somar, realizar média, etc.

-- COMMAND ----------

-- count()
 SELECT count(*) AS Number_of_Customers FROM dbacademy.basic_sql_for_databricks_sql_customers;


-- COMMAND ----------

-- sum()
SELECT sum(units_purchased) AS Total_California_Units FROM dbacademy.basic_sql_for_databricks_sql_customers WHERE state = 'CA';


-- COMMAND ----------

-- min(), max()
 SELECT min(discounted_price) AS Lowest_Discounted_Price, max(discounted_price) AS Highest_Discounted_Price FROM dbacademy.basic_sql_for_databricks_sql_silver_promo_prices;


-- COMMAND ----------

-- avg(), mean()
 SELECT avg(total_price) AS Mean_Total_Price from dbacademy.basic_sql_for_databricks_sql_sales_gold;


-- COMMAND ----------

-- std(), stddev()
 SELECT std(total_price) AS SD_Total_Price from dbacademy.basic_sql_for_databricks_sql_sales_gold;


-- COMMAND ----------

-- var_samp(), variance()
 SELECT variance(total_price) AS Variance_Total_Price from dbacademy.basic_sql_for_databricks_sql_sales_gold;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### Combinando funções
-- MAGIC 
-- MAGIC No exemplo será utilizada a combinação de funções, expressões regulares, para atingir um objetivo que consiste em verificar a correlação dos valores da moeda dolar com a sua quantidade, verificando se o comportamento do seu valor possui uma forte relação com a sua quantidade. 

-- COMMAND ----------

SELECT price FROM dbacademy.basic_sql_for_databricks_sql_silver_purchase_orders;


-- COMMAND ----------

 SELECT
   price AS Price,
   int(regexp_replace(price, '(\\$\\s)|(\\$)|(USD\\s)|(USD)', '')) AS Cleaned_USD_Price
 FROM
   dbacademy.basic_sql_for_databricks_sql_silver_purchase_orders
 WHERE
   price like '\$%'
   OR price like 'USD%';

-- COMMAND ----------

 SELECT
   corr(
     int(
       regexp_replace(price, '(\\$\\s)|(\\$)|(USD\\s)|(USD)', '')
     ),
     quantity
   ) AS Correlation_USD_Price_Quantity
 FROM
   dbacademy.basic_sql_for_databricks_sql_silver_purchase_orders
 WHERE
   price like '\$%'
   OR price like 'USD%';
