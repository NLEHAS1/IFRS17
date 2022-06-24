-- Databricks notebook source
Contracts_Controls_20191231_UPDATE20220425 ---> CoverEndDAte >20192131 ----> 20192131.20220425RUN127

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData")

-- COMMAND ----------

create table MI2022.Contracts_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/HarmonizedData/Contracts/*")


-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_20220425_RUN127

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_20220425_RUN127

-- COMMAND ----------

select distinct datasource from MI2022.Contracts_SL1_20191231_20220425_RUN127

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_20220425_RUN127 where riskperiodenddate>20191231 and datasource='SYM'

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_20220425_RUN127 where coverenddate>20191231 and datasource='SYM'

-- COMMAND ----------

select distinct valuationdate from MI2022.Contracts_SL1_20191231_20220425_RUN127

-- COMMAND ----------

create table MI2022.TBPO_POL_MODULES_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_POL_MODULES/*");

create table MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_REF_MODULE_VARIABLE_TYPES/*");


create table MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_POL_MOD_VARIABLES/*");


create table MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_POL_VERSIONS/*");

create table MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_CTRY_GRP_MCTS/*");

create table MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_CTRY_GRP_COVER_PCTS/*");

create table MI2022.TBBU_POLICIES_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBBU_POLICIES/*");

create table MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBOR_NON_NCM_ORGANISATIONS");

create table MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_20220425_RUN127
using orc
options (path="/mnt/sl1/DATA/SL1/20220425_RUN127/SourceData/CI/Symphony/TBPO_POL_GROUP_POLICIES/*")

-- COMMAND ----------

20191231.v20220529_01 

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/CI/Symphony/TBPO_POL_MODULES")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220529_01/SourceData/HarmonizedData/Contracts/") ---> CoverEndDate >= 20200101 --> ValuationDAte=20191231

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20200331.v20220529_01/SourceData/HarmonizedData/Contracts/") RiskPeriodEndDate > 20200331 --> VlauationDate=20200331
