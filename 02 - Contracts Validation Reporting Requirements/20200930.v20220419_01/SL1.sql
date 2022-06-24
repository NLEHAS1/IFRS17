-- Databricks notebook source
-- MAGIC %md #Contracts

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/HarmonizedData/Contracts/")

-- COMMAND ----------

create table MI2022.Contracts_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/HarmonizedData/Contracts/*")


-- COMMAND ----------

select * from MI2022.Contracts_SL1_20200930_v20220419_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200930_v20220419_01

-- COMMAND ----------

create temp view test as 
select distinct ValuationDate,	ContractId,	DataSource,	PolicyId,	ManagedTogetherId,	InsurerId,	InsuredId,	BeneficiaryId,	CustomerCountry,	CoverStartDate,	CoverEndDate,	BoundDate,	ContractIssueDate,	ContractInceptionDate,	RiskPeriodStartDate,	RiskPeriodEndDate,	Cancellability,	InitialProfitabilityClassing,	ProductType,	MainProduct,	Unit,	MainUnit from MI2022.Contracts_SL1_20200930_v20220419_01

-- COMMAND ----------

select count(*) from test

-- COMMAND ----------

desc extended MI2022.Contracts_SL1_20200930_v20220419_01

-- COMMAND ----------

select distinct valuationdate from MI2022.Contracts_SL1_20200930_v20220419_01

-- COMMAND ----------

select contractid,count(*) from MI2022.Contracts_SL1_20200930_v20220419_01 group by contractid having count(*)>1 order by 2 desc

-- COMMAND ----------

select contractid,count(contractid) from MI2022.Contracts_SL1_20200930_v20220419_01 group by contractid having count(contractid)>1 order by 2 desc

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_MODULES 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MODULES")

-- COMMAND ----------

create table MI2022.TBPO_POL_MODULES_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MODULES/*")

-- COMMAND ----------

select * from MI2022.TBPO_POL_MODULES_SL1_20200930_v20220419_01

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_MODULES_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_REF_MODULE_VARIABLE_TYPES 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES")

-- COMMAND ----------

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_MOD_VARIABLES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES")

-- COMMAND ----------

create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_MOD_VARIABLES_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_VERSIONS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS")

-- COMMAND ----------

create table MI2022.TBPO_POL_VERSIONS_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_VERSIONS_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_CTRY_GRP_MCTS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS")

-- COMMAND ----------

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_CTRY_GRP_COVER_PCTS

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS")

-- COMMAND ----------

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBBU_POLICIES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBBU_POLICIES")

-- COMMAND ----------

create table MI2022.TBBU_POLICIES_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBBU_POLICIES/*")

-- COMMAND ----------

select count(*) from MI2022.TBBU_POLICIES_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBOR_NON_NCM_ORGANISATIONS

-- COMMAND ----------

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS")

-- COMMAND ----------

select count(*) from MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20200930_v20220419_01

-- COMMAND ----------

-- MAGIC %md #TBPO_POL_GROUP_POLICIES

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES")

-- COMMAND ----------

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20200930_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220419_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES/*")

-- COMMAND ----------

select count(*) from MI2022.TBPO_POL_GROUP_POLICIES_SL1_20200930_v20220419_01
