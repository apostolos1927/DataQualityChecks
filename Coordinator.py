# Databricks notebook source
dbutils.widgets.multiselect("dataset", "dataset1", ["dataset1", "dataset2"])

dbutils.widgets.dropdown("env", "dev", ["dev","prd"])


# COMMAND ----------

class NotebookData:
  def __init__(self, path, params):
    self.path = path
    self.params = params

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

dataset= dbutils.widgets.get("dataset")
env = dbutils.widgets.get("env")

datasets = dataset.split(',')

my_list = []

for i in datasets:
  print(i)
  my_list.append(NotebookData("./Checks",{"dataset": i,"env":env}))


def submitNotebook(notebookt):
    print("Running notebook %s" % notebookt.path)
    print("Running notebook %s" % notebookt.params)
    try:
      dbutils.notebook.run(notebookt.path,3600,notebook.params)
    except Exception as e:
      print('exception is ',e)

numInParallel=len(datasets)

with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    for notebook in my_list:
      ec.submit(submitNotebook, notebook) 

# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows
