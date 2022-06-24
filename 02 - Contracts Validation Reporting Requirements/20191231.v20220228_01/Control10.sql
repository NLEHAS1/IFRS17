-- Databricks notebook source
-- MAGIC %md Based on Amir req
-- MAGIC 
-- MAGIC Report that shows RiskStartDate 12 months greater than the valuationDate and mainproduct='CI-ST'

-- COMMAND ----------

-- MAGIC %md If the code will apply to another schema make sure that RiskPeriodStartDate >20201231 is updated to match 12 after ValuationDate

-- COMMAND ----------

drop view Control10

-- COMMAND ----------

Create view MI2022.Control10 as 
select * from  MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM' and RiskPeriodEndDate > 20191231 and RiskPeriodStartDate >20201231 and MainProduct='CI-ST'

-- COMMAND ----------

select count(*) from Control10

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=1002380

-- COMMAND ----------

select * from  MI2022.Contracts_SL1_20191231_v20220228_01 where contractissuedate!=contractinceptiondate and datasource='SYM' 

