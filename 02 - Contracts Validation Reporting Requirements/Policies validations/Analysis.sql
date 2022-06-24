-- Databricks notebook source
SYM and CYC ---> Tbbu_policies, SL1.Contracts, Sl2.contracts on PolicyId

-- COMMAND ----------

-- MAGIC %md #SYM

-- COMMAND ----------

show tables in mi2022

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/CI/Symphony/TBBU_POLICIES")

-- COMMAND ----------

-- MAGIC %fs ls /mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/HarmonizedData/Contracts

-- COMMAND ----------

create table MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/CI/Symphony/TBBU_POLICIES");

-- COMMAND ----------

select count(*) from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01

-- COMMAND ----------

create table MI2022.Contracts_SL1_20201231_v20220329_01
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/HarmonizedData/Contracts/*")

-- COMMAND ----------

-- MAGIC %md #Anlaysis between source and SL1

-- COMMAND ----------

select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'
except
select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01

-- COMMAND ----------

-- MAGIC %md number of records missing from Contracts SL1

-- COMMAND ----------

select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'

-- COMMAND ----------

select count(distinct id) from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01

-- COMMAND ----------

select count(distinct policyid) from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'

-- COMMAND ----------

select distinct datasource from MI2022.Contracts_SL1_20201231_v20220329_01

-- COMMAND ----------

with CTE as (
  select
    distinct id
  from
    MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
  except
  select
    distinct policyid
  from
    MI2022.Contracts_SL1_20201231_v20220329_01
  where
    datasource = 'SYM'
)
select
  count(*)
from
  CTE

-- COMMAND ----------

-- MAGIC %md The excluded policies have the following status which are all the status of policies

-- COMMAND ----------

with CTE as (select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM')
select distinct status_code from CTE a inner join MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01 b on a.id=b.id

-- COMMAND ----------

-- MAGIC %md Start and Risk End dates have no pattern

-- COMMAND ----------

with CTE as (select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM')
select distinct b.d_popvn_end_risk_dat from CTE a inner join MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01 b on a.id=b.id order by 1 desc


-- COMMAND ----------

with CTE as (select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM')
select distinct b.d_popvn_start_risk_dat from CTE a inner join MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01 b on a.id=b.id order by 1 asc


-- COMMAND ----------

with CTE as (select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM')
select * from CTE a inner join MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01 b on a.id=b.id

-- COMMAND ----------



-- COMMAND ----------

with CTE as (select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM')
select count(*) from CTE

-- COMMAND ----------

select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01
except
select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM'

-- COMMAND ----------

select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM'
except
select distinct id from MI2022.TBBU_POLICIES_SL1_20201231_v20220329_01

-- COMMAND ----------

-- MAGIC %md #Between SL1 and SL2

-- COMMAND ----------

select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM'
except
select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'

-- COMMAND ----------

-- MAGIC %md the number of records missing in SL2

-- COMMAND ----------

select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'
except
select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM'

-- COMMAND ----------

with CTE as (select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'
except
select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM')
select count(*) from CTE

-- COMMAND ----------

-- MAGIC %md No patterns

-- COMMAND ----------

with CTE as (select distinct policyid from MI2022.Contracts_SL1_20201231_v20220329_01 where datasource='SYM'
except
select distinct policyid from MI2022.contracts_202012_20220413_140845_207 where datasource='SYM')
select b.* from CTE a inner join MI2022.Contracts_SL1_20201231_v20220329_01 b on a.policyid=b.policyid

-- COMMAND ----------

select * from MI2022.contracts_202012_20220413_140845_207

-- COMMAND ----------

select count(distinct policyid) from  MI2022.contracts_202012_20220413_140845_207 where datasource='SYM'
