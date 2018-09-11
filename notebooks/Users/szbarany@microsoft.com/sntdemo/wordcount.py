# Databricks notebook source

# Creating widgets for leveraging parameters, and printing the parameters

dbutils.widgets.text("input", "","")
dbutils.widgets.get("input")
y = getArgument("input")
print "Param -\'input':"
print y



# COMMAND ----------

import datetime
print datetime.datetime.now().strftime('%Y%m%d%H%M')

# COMMAND ----------

import datetime

dbutils.widgets.text("inputfile", "","")
dbutils.widgets.get("inputfile")
paramfile = getArgument("inputfile")
print "Param -\'inputfile':"
print paramfile
#  beallitjuk a storage keyt mint kornyezeti valtozo
spark.conf.set(
  "fs.azure.account.key.bszadfdemostore.blob.core.windows.net",
  "/iuYFsJ4SnNuHGoESnAIbxy1OUwa2YiLETUTJtX4uxubA20cPACFdKaPlrQGLlimNxhpcAfnrFOEgE/2Vm3+eA==")
#blob storage felmountolas
#ellenorizzuk fel van e mountolva
mountings = dbutils.fs.mounts()
felmountolva = False
for s in mountings:
  if str(s.mountPoint)== "/mnt/sntblob":
    felmountolva = bool(felmountolva + True)

if felmountolva == False:
  dbutils.fs.mount("wasbs://onpremcopy@bszadfdemostore.blob.core.windows.net/dbrick",
  mount_point = "/mnt/sntblob",
  extra_configs = {"fs.azure.account.key.bszadfdemostore.blob.core.windows.net": "/iuYFsJ4SnNuHGoESnAIbxy1OUwa2YiLETUTJtX4uxubA20cPACFdKaPlrQGLlimNxhpcAfnrFOEgE/2Vm3+eA=="})
else: 
    print("minden ok")  
    dbutils.fs.refreshMounts()
    datefile = "teszt_"+str(datetime.date.today())+".txt"
    with open("/dbfs/mnt/sntblob/dbrick/"+paramfile, 'w') as f_write:
  # read the file
      with open("/dbfs/mnt/sntblob/stagefile.txt", "r") as f_read:
        for line in f_read:
          f_write.write(line)

# COMMAND ----------

#  beallitjuk a storage keyt mint kornyezeti valtozo
spark.conf.set(
  "fs.azure.account.key.bszadfdemostore.blob.core.windows.net",
  "/iuYFsJ4SnNuHGoESnAIbxy1OUwa2YiLETUTJtX4uxubA20cPACFdKaPlrQGLlimNxhpcAfnrFOEgE/2Vm3+eA==")
#blob storage felmountolas
#ellenorizzuk fel van e mountolva
mountings = dbutils.fs.mounts()
felmountolva = False
for s in mountings:
  if str(s.mountPoint)== "/mnt/sntblob":
    felmountolva = bool(felmountolva + True)

if felmountolva == False:
  dbutils.fs.mount("wasbs://onpremcopy@bszadfdemostore.blob.core.windows.net/dbrick",
  mount_point = "/mnt/sntblob",
  extra_configs = {"fs.azure.account.key.bszadfdemostore.blob.core.windows.net": "/iuYFsJ4SnNuHGoESnAIbxy1OUwa2YiLETUTJtX4uxubA20cPACFdKaPlrQGLlimNxhpcAfnrFOEgE/2Vm3+eA=="})
else: 
    print("minden ok")  
    dbutils.fs.refreshMounts()

# COMMAND ----------

mountings = dbutils.fs.mounts()
for s in mountings:
   print str(s.mountPoint)

# COMMAND ----------

dbutils.fs.unmount("/mnt/eonblob3")
dbutils.fs.unmount("/mnt/eonblob")

# COMMAND ----------

dbutils.fs.mkdirs("/dbfs/mnt/sntblob/foobar/")
dbutils.fs.put("/dbfs/mnt/sntblob/foobar/testfile.txt", "Hello, World!")

# COMMAND ----------

with open("/dbfs/mnt/sntblob/testfile.txt", 'w') as f:
      f.write("File iras teszt  parameterrel :")
      f.write("parameter vege\n")
  # read the file
with open("/dbfs/mnt/sntblob/testfile.txt", "r") as f_read:
      for line in f_read:
        print (line)