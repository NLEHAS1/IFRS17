-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/ICP") 

-- COMMAND ----------

create table sl1_20201231_v20220529_03.ICP_SAS
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/ICP/ICP_SAS/*");

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ICP_SAS
