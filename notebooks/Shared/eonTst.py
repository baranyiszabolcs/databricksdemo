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
# Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("eonpic", "","")
dbutils.widgets.get("eonpic")
y = getArgument("eonpic")
print ("Param -\'eonpic':")
print (y)
datefile = "teszt_"+str(datetime.date.today())+".txt"
with open("/dbfs/mnt/eonblob/foobar/"+datefile, 'a') as f:
    f.write("File iras teszt  parameterrel :" + str(y))
    f.write("parameter vege\n")
  # read the file
with open("/dbfs/mnt/eonblob/foobar/"+datefile, "r") as f_read:
    for line in f_read:
       print (line)