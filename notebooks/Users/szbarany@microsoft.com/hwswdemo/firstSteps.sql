-- Databricks notebook source
-- MAGIC %md 
-- MAGIC Create a table from a Databricks dataset  using  built in ataset

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds
USING csv
OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")


-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
-- MAGIC diamonds.write.format("delta").save("/delta/diamonds")

-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;

CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

-- COMMAND ----------

SELECT * from diamonds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results 
-- MAGIC 
-- MAGIC Specifically, the command:
-- MAGIC 1. Selects color and price columns, averages the price, and groups and orders by color.
-- MAGIC 1. Displays a table of the results.

-- COMMAND ----------

SELECT color, avg(price) AS price FROM diamonds GROUP BY color ORDER BY color

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Convert the table to a chart
-- MAGIC 
-- MAGIC Under the table, click the bar chart <img src="http://docs.databricks.com/_static/images/notebooks/chart-button.png"/></a> icon. 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Repeat the same operations using Python DataFrame API. 
-- MAGIC This is a SQL notebook; by default command statements are passed to a SQL interpreter. To pass command statements to a Python interpreter, include the `%python` magic command.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command creates a DataFrame from a Databricks dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The next command manipulates the data and displays the results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import avg
-- MAGIC 
-- MAGIC display(diamonds.select("color","price").groupBy("color").agg(avg("price")).sort("color"))