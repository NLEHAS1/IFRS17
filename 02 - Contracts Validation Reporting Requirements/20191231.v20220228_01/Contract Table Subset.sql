-- Databricks notebook source
drop view MI2022.ContractsSubset

-- COMMAND ----------

Create view MI2022.ContractsSubset as
select * from MI2022.Contracts_SL1_20191231_v20220228_01 where DataSource='SYM' and RiskPeriodEndDate>20191231

-- COMMAND ----------

select count(*) from MI2022.ContractsSubset
