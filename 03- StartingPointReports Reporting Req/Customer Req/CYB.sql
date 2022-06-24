-- Databricks notebook source
use db_201912_20220620_083726_623

-- COMMAND ----------

select distinct datasource from contracts

-- COMMAND ----------

select * from contracts where datasource='CYB'

-- COMMAND ----------

show tables in sl1_20191231_v20220529_03

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_03//SourceData/CI/CyC_SAS/ASEGURADO_CAUCION")

-- COMMAND ----------

create table sl1_20191231_v20220529_03.ASEGURADO_CAUCION
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_03//SourceData/CI/CyC_SAS/ASEGURADO_CAUCION/*")


-- COMMAND ----------

select * from sl1_20191231_v20220529_03.ASEGURADO_CAUCION where co_poliza=40040042 and current='Y'

-- COMMAND ----------

select nu_nif_empresa as CustomerId,no_empresa as CustomerName,a.* from db_201912_20220620_083726_623.contracts a left join sl1_20191231_v20220529_03.ASEGURADO_CAUCION b on (a.policyid=b.co_poliza and current='Y') where datasource='CYB'

-- COMMAND ----------

with CTE as
(select nu_nif_empresa as CustomerId,no_empresa as CustomerName,a.* from db_201912_20220620_083726_623.contracts a left join sl1_20191231_v20220529_03.ASEGURADO_CAUCION b on (a.policyid=b.co_poliza and current='Y') where datasource='CYB')
select contractid,count(*) from CTE  group by contractid having count(*)>1 order by 2 desc
