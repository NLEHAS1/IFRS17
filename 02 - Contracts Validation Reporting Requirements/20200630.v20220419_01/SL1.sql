-- Databricks notebook source
-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/HarmonizedData/Contracts/")

-- COMMAND ----------

create table MI2022.Contracts_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/HarmonizedData/Contracts/*")


-- COMMAND ----------

select * from MI2022.Contracts_SL1_20200630_v20220419_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200630_v20220419_01

-- COMMAND ----------

select contractid,count(*) from MI2022.Contracts_SL1_20200630_v20220419_01 where valuationdate='20200630'  group by contractid having count(*)>1 order by 2 desc

-- COMMAND ----------

select distinct valuationdate from MI2022.Contracts_SL1_20200630_v20220419_01

-- COMMAND ----------

create table MI2022.TBPO_POL_MODULES_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MODULES/*");

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES/*");


create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES/*");


create table MI2022.TBPO_POL_VERSIONS_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS/*");

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS/*");

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS/*");

create table MI2022.TBBU_POLICIES_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBBU_POLICIES/*");

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS");

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20200630_v20220419_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220419_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES/*")


