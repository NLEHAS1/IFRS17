-- Databricks notebook source
select count(*) from MI2022.Contracts_SL1_20200331_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(distinct policyid) from MI2022.Contracts_SL1_20200331_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200630_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(distinct policyid) from MI2022.Contracts_SL1_20200630_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200930_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(distinct policyid) from MI2022.Contracts_SL1_20200930_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20201231_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(distinct policyid) from MI2022.Contracts_SL1_20201231_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>ValuationDate

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200331_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>20191231

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200630_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>20191231

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200930_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>20191231

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20201231_v20220419_01 where datasource='SYM' and RiskPeriodEndDate>20191231
