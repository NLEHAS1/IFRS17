-- Databricks notebook source
-- ---> CoverEndDate >= 20200101 --> ValuationDAte=20191231

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC display(dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/HarmonizedData/Contracts/"))

-- COMMAND ----------

desc extended MI2022.Contracts_SL1_20191231_v20220529_01

-- COMMAND ----------

desc extended mireporting.test

-- COMMAND ----------

show grant on  mireporting

-- COMMAND ----------

create table MI2022.Contracts_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/HarmonizedData/Contracts/*");

create table MI2022.TBPO_POL_MODULES_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_POL_MODULES/*");

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES/*");


create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES/*");


create table MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS/*");

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS/*");

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS/*");

create table MI2022.TBBU_POLICIES_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBBU_POLICIES/*");

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS");

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES/*")

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220529_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220529_01
