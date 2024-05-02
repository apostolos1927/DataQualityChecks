# Databricks notebook source
dbutils.widgets.dropdown("dataset", "dataset1", 
                         ["dataset1", "dataset2"], "Data feed")
dataset= dbutils.widgets.get("dataset")

dbutils.widgets.dropdown("env", "prd", ["dev","prd"])
env= dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC Check the number of records on each layer

# COMMAND ----------

# MAGIC %sql
# MAGIC  with bronze as (
# MAGIC         SELECT '${dataset}' as dataFeed,
# MAGIC                 'bronze'as layer,
# MAGIC                  count(*) as CNT
# MAGIC                  FROM delta.`/mnt/demo/${env}_bronze/${dataset}`
# MAGIC ),
# MAGIC silver as (
# MAGIC         SELECT '${dataset}' as dataFeed,
# MAGIC                 'silver'as layer,
# MAGIC                 count(*) as CNT
# MAGIC         FROM delta.`/mnt/demo/${env}_silver/${dataset}`
# MAGIC         )
# MAGIC select dataFeed, layer, CNT from bronze
# MAGIC union all
# MAGIC select dataFeed, layer, CNT from silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from range(-30,1,1) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select date_format(date_add(date(NOW()),int(id)),'yyyyMMdd') as datekeys from range(-30,1,1) 

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC silver as (
# MAGIC         SELECT '${dataset}' as dataFeed,
# MAGIC                 'silver'as layer,
# MAGIC                 DateKey
# MAGIC         FROM delta.`/mnt/demo/${env}_silver/${dataset}`
# MAGIC         where DateKey>= date_format(date(NOW())-15,'yyyyMMdd')
# MAGIC
# MAGIC ),
# MAGIC dates as (
# MAGIC    select date_format(date_add(date(NOW()),int(id)),'yyyyMMdd') as datekeys from range(-30,1,1) 
# MAGIC )
# MAGIC ,silver_cte as (
# MAGIC select dataFeed, layer, DateKey, count(*) as CNT from silver
# MAGIC group by dataFeed, layer, DateKey
# MAGIC )
# MAGIC
# MAGIC select * from dates a
# MAGIC right join silver_cte b on a.datekeys=b.DateKey
# MAGIC order by a.datekeys desc

# COMMAND ----------

df = spark.sql(f""" 
with bronze as (
     SELECT '${dataset}' as dataFeed,
                'bronze'as layer,
                DateKey
                 FROM delta.`/mnt/demo/{env}_bronze/{dataset}`
                          )
,dates as (
     select date_format(date_add(date(NOW()),int(id)),'yyyyMMdd') as datakeys from range(-30,1,1) 
 )
,bronze_cte as (
     select dataFeed, layer, DateKey, count(*) as BRONZE_CNT from bronze
     group by dataFeed, layer, DateKey
)
,silver as (
       SELECT '${dataset}' as dataFeed,
                'silver'as layer,
                DateKey
        FROM delta.`/mnt/demo/{env}_silver/{dataset}`
        where DateKey>= date_format(date(NOW())-15,'yyyyMMdd')
)
,silver_cte as (
select dataFeed, layer, DateKey, count(*) as SILVER_CNT from silver
group by dataFeed, layer, DateKey
)

                  select a.*
                         ,C.dataFeed
                         ,b.BRONZE_CNT
                         ,c.SILVER_CNT
                         ,b.BRONZE_CNT- c.SILVER_CNT AS DIFF
                  from dates a
                  right join bronze_cte b on a.datakeys=b.DateKey
                  right join silver_cte c on a.datakeys=c.DateKey
                  order by a.datakeys desc
""")

# COMMAND ----------

df.display()

# COMMAND ----------

print(df.select(df.DIFF).groupBy().sum().collect()[0][0])

# COMMAND ----------

threshold = 0
if abs(df.select(df.DIFF).groupBy().sum().collect()[0][0])>threshold:
  raise Exception ('Check errors')
else:
  dbutils.notebook.exit("success")  
