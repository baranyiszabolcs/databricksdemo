# Databricks notebook source
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
    datefile = "_"+str(datetime.datetime.now())+".txt"
    with open("/dbfs/mnt/sntblob/dbrick/"+paramfile+datefile, 'w') as f_write:
  # read the file
      with open("/dbfs/mnt/sntblob/stagefile.txt", "r") as f_read:
        for line in f_read:
          f_write.write(line)
print("completed")