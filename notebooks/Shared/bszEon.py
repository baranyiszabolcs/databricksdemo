# Databricks notebook source
import datetime

#  beallitjuk a storage keyt mint kornyezeti valtozo
spark.conf.set(
  "fs.azure.account.key.bszeonstr.blob.core.windows.net",
  "P1pfdefp45qDMca5Y9VtN1QchJPTMf/qnGOTQpHd8HtbnIbMS/eWHdaN9I1nFy0hDwGzQ38RzwSIRPJQyxaRTQ==")
#blob storage felmountolas
#ellenorizzuk fel van e mountolva
mountings = dbutils.fs.mounts()
felmountolva = False
for s in mountings:
  if str(s.mountPoint)== "/mnt/eonblob3":
    felmountolva = bool(felmountolva + True)

if felmountolva == False:
  dbutils.fs.mount("wasbs://pyteszt@bszeonstr.blob.core.windows.net/sample",
  mount_point = "/mnt/eonblob3",
  extra_configs = {"fs.azure.account.key.bszeonstr.blob.core.windows.net": "P1pfdefp45qDMca5Y9VtN1QchJPTMf/qnGOTQpHd8HtbnIbMS/eWHdaN9I1nFy0hDwGzQ38RzwSIRPJQyxaRTQ=="})
else: 
    print("minden ok")  
    dbutils.fs.refreshMounts()
    datefile = "teszt_"+str(datetime.date.today())+".txt"
    with open("/dbfs/mnt/eonblob/foobar/"+datefile, 'a') as f:
      f.write("File iras teszt  parameterrel :")
      f.write("parameter vege\n")
  # read the file
    with open("/dbfs/mnt/eonblob/foobar/"+datefile, "r") as f_read:
      for line in f_read:
        print (line)
    



# COMMAND ----------

#dbutils.fs.unmount("{mountPointPath}")
#dbutils.fs.ls("/mnt/eonblob")
#dbutils.fs.mkdirs("mnt/eonblob/foobar/")
#dbutils.fs.put("mnt/eonblob/foobar/baz.txt", "Hello, World!",True)
#display(dbutils.fs.ls("dbfs:/mnt/eonblob/foobar"))


#python
# write a file to DBFS using python i/o apis
with open("/dbfs/mnt/eonblob/foobar/bazi.txt", 'a') as f:
  f.write("Apache Spark is awesome!\n")
  f.write("End of example!")

# read the file
with open("/dbfs/mnt/eonblob/foobar/bazi.txt", "r") as f_read:
  for line in f_read:
    print (line)



# COMMAND ----------

mountings = dbutils.fs.mounts()
for s in mountings:
  if str(s.mountPoint)== "/mnt/eonblo":
    print("mar van")
  else:
    print(str(s.mountPoint))
print(mountings)

# COMMAND ----------

mountings = dbutils.fs.mounts()
felmountolva = False
for s in mountings:
  if str(s.mountPoint)== "/mnt/eonblob":
    felmountolva = bool(felmountolva + True)
    
if felmountolva == False:
  dbutils.fs.mount("wasbs://pyteszt@bszeonstr.blob.core.windows.net/sample",
  mount_point = "/mnt/eonblob",
  extra_configs = {"fs.azure.account.key.bszeonstr.blob.core.windows.net": "P1pfdefp45qDMca5Y9VtN1QchJPTMf/qnGOTQpHd8HtbnIbMS/eWHdaN9I1nFy0hDwGzQ38RzwSIRPJQyxaRTQ=="})
else: 
    print("minden ok")


# COMMAND ----------

#lemountolas
dbutils.fs.unmount("/mnt/eonblob3")

# COMMAND ----------

import datetime
datefile = "teszt_"+str(datetime.date.today())+".txt"
dbutils.fs.put("mnt/eonblob/"+datefile, "Hello, World!"+str(datetime.datetime.now()),True)




# COMMAND ----------

dbutils.fs.help()