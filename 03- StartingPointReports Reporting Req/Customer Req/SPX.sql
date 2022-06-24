-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_03/SourceData/SP/SpecialProducts")

-- COMMAND ----------

select * from db_201912_20220620_083726_623.contracts where datasource='SPX'

-- COMMAND ----------

-- MAGIC %md According to Dispesh Vajah, the insuredid is the customer id and customer name can be found in organisation details

-- COMMAND ----------

create table sl1_20191231_v20220529_03.BOUND_POLICY_DATA
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_03/SourceData/SP/SpecialProducts/BOUND_POLICY_DATA/*")

-- COMMAND ----------

select * from sl1_20191231_v20220529_03.BOUND_POLICY_DATA where policy_number0=967271

-- COMMAND ----------

select id as CustomerId,d_ornol_short_name as CustomerName, a.* from db_201912_20220620_083726_623.contracts a
    left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (
      InsuredId = Id
      and b.effect_to_dat > current_date())  where datasource='SPX'

-- COMMAND ----------

with CTE as (
select id as CustomerId,d_ornol_short_name as CustomerName, a.* from db_201912_20220620_083726_623.contracts a
    left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (
      InsuredId = Id
      and b.effect_to_dat > current_date())  where datasource='SPX'
)

select contractid,count(*) from CTE  group by contractid having count(*)>1 order by 2 desc
