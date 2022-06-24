-- Databricks notebook source
drop view MI2022.Control8

-- COMMAND ----------

create view MI2022.Control8 as 
select * from MI2022.Contracts_SL1_20191231_v20220228_01  where Cancellability!=0 and RiskPeriodEndDate > 20191231 and DataSource='SYM'

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0

-- COMMAND ----------

select count(*) from MI2022.Control8
