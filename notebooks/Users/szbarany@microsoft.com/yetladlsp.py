# Databricks notebook source
# mount adls to DFS  into  mntadls
#  ir requires to create a technical application account in AD  and give right to this account
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "6a58dc58-bd3f-4eca-8e67-cc628bbf5975",
           "dfs.adls.oauth2.credential": "Cz5sxW7fNkF7dlK9hSvH5Egpleaao1zwh9ey1QxCAKE=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}
try:
  dbutils.fs.mount(  source = "adl://yieldegoadls.azuredatalakestore.net",  mount_point = "/mnt/adls",  extra_configs = configs)
except:
  dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE adls_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC CREATE  TABLE adls_transactions
# MAGIC ( site_id BIGINT ,article_id LONG, quantity DOUBLE, date DATE, price DOUBLE,supplier_price DOUBLE,promo_type_id INT)
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/mnt/adls/samplev1/transactions_all_merged.csv", header "true", mode "FAILFAST")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view adls_transaction_aggregate AS
# MAGIC SELECT 
# MAGIC     tr.site_id                                                    as SiteId, 
# MAGIC     tr.date                                                       as Datum, 
# MAGIC     ROUND(SUM((tr.supplier_price) * tr.quantity), 2)            as Costs,
# MAGIC     ROUND(SUM(tr.quantity), 2)                                    as Quantity,
# MAGIC     ROUND(SUM(tr.quantity * tr.price), 2)                       as Revenue,
# MAGIC     ROUND(SUM(CASE WHEN tr.promo_type_id IS NULL THEN (tr.supplier_price) * tr.quantity ELSE 0 END), 2) as RegularCosts,
# MAGIC     ROUND(SUM(CASE WHEN tr.promo_type_id IS NULL THEN tr.quantity ELSE 0 END), 2)                         as RegularQuenatity,
# MAGIC     ROUND(SUM(CASE WHEN tr.promo_type_id IS NULL THEN tr.quantity * tr.price ELSE 0 END), 2)            as RegularRevenue
# MAGIC FROM adls_transactions tr
# MAGIC GROUP BY tr.site_id, tr.date

# COMMAND ----------

resultDF = spark.sql("select * from adls_transaction_aggregate")
resultDF.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("/mnt/adls/adls_transaction_aggregate.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE adls_articles

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  TABLE adls_articles
# MAGIC ( id BIGINT ,pivot_category_id INT)
# MAGIC   USING csv
# MAGIC   OPTIONS (path "/mnt/adls/samplev1/articles.csv", header "false", mode "FAILFAST")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  or replace  VIEW adls_articles_tr_aggr AS 
# MAGIC SELECT 
# MAGIC     ar.pivot_category_id                                          , 
# MAGIC     tr.site_id                                                    , 
# MAGIC     tr.date                                                       ,
# MAGIC     SUM(tr.quantity * tr.supplier_price)              as Costs,
# MAGIC     SUM(tr.quantity)                                    as Quantity,
# MAGIC     SUM(tr.quantity * tr.price)                     as Revenue,
# MAGIC     SUM(CASE WHEN tr.promo_type_id IS NULL THEN tr.quantity * tr.supplier_price ELSE 0 END)     as RegularCosts,
# MAGIC     SUM(CASE WHEN tr.promo_type_id IS NULL THEN tr.quantity ELSE 0 END)                           as RegularQuantity,
# MAGIC     SUM(CASE WHEN tr.promo_type_id IS NULL THEN tr.quantity * tr.price ELSE 0 END)              as RegularRevenue
# MAGIC FROM adls_TRANSACTIONS tr 
# MAGIC JOIN adls_articles ar ON ar.id = tr.article_id
# MAGIC WHERE ar.pivot_category_id IS NOT NULL
# MAGIC GROUP BY ar.pivot_category_id, tr.site_id, tr.date

# COMMAND ----------

resultDF = spark.sql("select * from adls_articles_tr_aggr")
resultDF.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").save("adls_articles_tr_aggr")