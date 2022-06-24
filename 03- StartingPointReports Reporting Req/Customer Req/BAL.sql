-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_03/SourceData/InwardReinsurance/Balloon")

-- COMMAND ----------

select * from db_201912_20220620_083726_623.contracts where datasource='BAL'
