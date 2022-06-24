-- Databricks notebook source
Contracts_Controls_20191231_UPDATE20220425 ---> CoverEndDAte >20192131 ----> 20192131.20220425RUN127

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI")

-- COMMAND ----------

create schema IF NOT EXISTS MI2022

-- COMMAND ----------

-- MAGIC %md # Contracts

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/HarmonizedData/Contracts")

-- COMMAND ----------

create table MI2022.Contracts_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/HarmonizedData/Contracts")


-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM'

-- COMMAND ----------

show tables in MI2022

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

select  count( distinct ValuationDate,	ContractId,	DataSource,	PolicyId,	ManagedTogetherId,	InsurerId,	InsuredId,	BeneficiaryId,	CustomerCountry,	CoverStartDate,	CoverEndDate,	BoundDate,	ContractIssueDate,	ContractInceptionDate,	RiskPeriodStartDate,	RiskPeriodEndDate,	Cancellability,	InitialProfitabilityClassing,	ProductType,	MainProduct,	Unit,	MainUnit) from MI2022.Contracts_SL1_20191231_v20220228_01


-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

desc extended MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

select distinct valuationdate from MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

select contractid,count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 group by contractid having count(*)>1 order by 2 desc

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where contractid='SPX / 419633 / 20160714'

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_MODULES 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_MODULES")

-- COMMAND ----------

create table MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_MODULES")

-- COMMAND ----------

select * from MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_REF_MODULE_VARIABLE_TYPES 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES")

-- COMMAND ----------

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES")

-- COMMAND ----------

select count(*) from MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_MOD_VARIABLES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES")

-- COMMAND ----------

create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_VERSIONS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS")

-- COMMAND ----------

create table MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_CTRY_GRP_MCTS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS")

-- COMMAND ----------

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS")

-- COMMAND ----------

select count(*) from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_CTRY_GRP_COVER_PCTS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS")

-- COMMAND ----------

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS")

-- COMMAND ----------

select count(*) from MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBIF_REF_OBJ_TYP

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ_TYP")

-- COMMAND ----------

create table MI2022.TBIF_REF_OBJ_TYP_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ_TYP")

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_TYP_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBIF_REF_OBJ_MAPPING

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ_MAPPING")

-- COMMAND ----------

create table MI2022.TBIF_REF_OBJ_MAPPING_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ_MAPPING")

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_MAPPING_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBIF_REF_OBJ

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ")

-- COMMAND ----------

create table MI2022.TBIF_REF_OBJ_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBIF_REF_OBJ")

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_SL1_20191231_v20220228_01

-- COMMAND ----------

show tables in mi2022

-- COMMAND ----------

select *from mi2022.asegurado_20191231 where nu_nif_empresa='B60689783' and current='Y'


-- COMMAND ----------

 select  nu_nif_empresa, count(distinct no_empresa) from mi2022.asegurado_20191231 where current='Y' group by nu_nif_empresa having count(distinct no_empresa)>1  order by 2 desc

-- COMMAND ----------

 select  no_empresa, count(distinct nu_nif_empresa) from mi2022.asegurado_20191231 where current='Y' and no_empresa is not null group by no_empresa having count(distinct nu_nif_empresa)>1  order by 2 desc

-- COMMAND ----------

select *from mi2022.asegurado_20191231 where no_empresa='ALMACENES METALURGICOS S A' and current='Y'


-- COMMAND ----------

select *from mi2022.asegurado_20191231 where nu_nif_empresa='B60684305' and current='Y'


-- COMMAND ----------

-- MAGIC %md #TBBU_POLICIES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBBU_POLICIES")

-- COMMAND ----------

create table MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBBU_POLICIES")

-- COMMAND ----------

select count(*) from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01

-- COMMAND ----------

select a.PolicyId,b.Id,a.RiskPeriodStartDate,a.CoverStartDate,b.d_popvn_start_risk_dat,a.RiskPeriodEndDate,a.CoverEndDate,b.d_popvn_end_risk_dat from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.policyid=b.id where b.d_popvn_start_risk_dat=b.d_popvn_end_risk_dat

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=358407

-- COMMAND ----------

select id,d_popvn_start_risk_dat,d_popvn_end_risk_dat from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=358407

-- COMMAND ----------

-- MAGIC %md #TBOR_NON_NCM_ORGANISATIONS

-- COMMAND ----------

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS")

-- COMMAND ----------

select * from MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_GROUP_POLICIES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES")

-- COMMAND ----------

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01

-- COMMAND ----------

-- MAGIC %md #TBPO_BUNDLE_POLICIES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_BUNDLE_POLICIES")

-- COMMAND ----------

create table MI2022.TBPO_BUNDLE_POLICIES_SL1_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI/Symphony/TBPO_BUNDLE_POLICIES")

-- COMMAND ----------

select * from  MI2022.TBPO_BUNDLE_POLICIES_SL1_20191231_v20220228_01

-- COMMAND ----------



-- COMMAND ----------

with alias(BId) as (
  select
    id
  from
    MI2022.TBPO_BUNDLE_POLICIES_SL1_20191231_v20220228_01
)
select
  *
from
  alias

-- COMMAND ----------

with test as (
select id from  MI2022.TBPO_BUNDLE_POLICIES_SL1_20191231_v20220228_01) 

SELECT *
FROM test



-- COMMAND ----------

-- MAGIC %sql
-- MAGIC WITH CTE(one, two) AS
-- MAGIC (
-- MAGIC     SELECT 1, 2
-- MAGIC )
-- MAGIC SELECT one, two, one + two as three from CTE

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in db_sl1_20211001_run13


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData")

-- COMMAND ----------

-- MAGIC %md #Expenses

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/Expenses/Tagetik")

-- COMMAND ----------

create table MI2022.Expenses_20191231_v20220228_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/Expenses/Tagetik")

-- COMMAND ----------

select * from MI2022.Expenses_20191231_v20220228_01
