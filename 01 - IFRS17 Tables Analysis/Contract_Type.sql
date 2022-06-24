-- Databricks notebook source
select count(*) from  Mi2022.CONTRACT_TYPE_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 where rein_contract_flg is null

-- COMMAND ----------

select distinct contractid from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206
except
select distinct contractid from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

select distinct contractid from MI2022.contracts_201912_20220413_133443_206
except
select distinct contractid from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from (
select distinct contractid from MI2022.contracts_201912_20220413_133443_206
except
select distinct contractid from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206)

-- COMMAND ----------

select * from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 where contractid='CYC / 30061964 / 20210601' 

-- COMMAND ----------

select * from MI2022.contracts_201912_20220413_133443_206 where contractid='CYC / 30061964 / 20210601'

-- COMMAND ----------

select count(distinct contractid) from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206

-- COMMAND ----------

select count(distinct contractid) from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

select count( distinct a.contractid) from MI2022.contracts_201912_20220413_133443_206 a inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on a.contractid=b.contractid

-- COMMAND ----------

select distinct CoverStartDate, CoverEndDate,ContractInceptionDate,ContractIssueDate,RiskPeriodStartDate,RiskPeriodEndDate from MI2022.contracts_201912_20220413_133443_206 a inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on a.contractid=b.contractid order by CoverStartDate, CoverEndDate,ContractInceptionDate,ContractIssueDate,RiskPeriodStartDate,RiskPeriodEndDate asc

-- COMMAND ----------

select distinct datasource from MI2022.contracts_201912_20220413_133443_206 a inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on a.contractid=b.contractid

-- COMMAND ----------

desc MI2022.contracts_201912_20220413_133443_206
