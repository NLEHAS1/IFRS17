-- Databricks notebook source
RiskPeriodEndDate > 20200331 --> VlauationDate=20200331

-- COMMAND ----------

create table MI2022.Contracts_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/HarmonizedData/Contracts/*");


create table MI2022.TBPO_POL_MODULES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_POL_MODULES/*");

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES/*");


create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES/*");


create table MI2022.TBPO_POL_VERSIONS_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_POL_VERSIONS/*");

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS/*");

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS/*");

create table MI2022.TBBU_POLICIES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBBU_POLICIES/*");

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS");

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES/*")

-- COMMAND ----------

select distinct valuationdate from MI2022.Contracts_SL1_20200331_v20220529_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20200331_v20220529_01

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBOR_CURRENCY_EXCHANGE_RATES/") 

-- COMMAND ----------

create table MI2022.TBOR_CURRENCY_EXCHANGE_RATES_SL1_20200331_v20220529_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/CI/Symphony/TBOR_CURRENCY_EXCHANGE_RATES/*")

-- COMMAND ----------

select * from MI2022.TBOR_CURRENCY_EXCHANGE_RATES_SL1_20200331_v20220529_01 where orcuy_code='PLN' and orcuy_code_base='EUR' and current_Date() between effect_from_dat AND effect_to_dat and typ = 'VAR'
