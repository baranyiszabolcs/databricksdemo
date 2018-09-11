# Databricks notebook source
# Creating widgets for leveraging parameters, and printing the parameters
# notebook job eseten tudom ezzel atvenni

dbutils.widgets.text("input", "","")
dbutils.widgets.get("input")
y = getArgument("input")
print "Param -\'input':"
print y


# COMMAND ----------

#command line parameterek atvetele
import sys

sys.argv
print ('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', str(sys.argv))

# COMMAND ----------

dbutils.widgets.help("text")