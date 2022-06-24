-- Databricks notebook source
show tables in sl1_20201231_v20220529_03

-- COMMAND ----------

create table sl1_20201231_v20220529_03.Contracts
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/HarmonizedData/Contracts/*");

-- COMMAND ----------

use sl1_20201231_v20220529_03

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from  Contracts where mainunit like 'GLB%' and MainProduct not like 'CI%'

-- COMMAND ----------

select * from Contracts where mainunit like 'LOC%' and MainProduct not like 'CI%'

-- COMMAND ----------

select * from contracts where mainproduct is null

-- COMMAND ----------

show tables in sl1_20200930_v20220529_03

-- COMMAND ----------

create table sl1_20200930_v20220529_03.Contracts
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220529_03/SourceData/HarmonizedData/Contracts/*");

-- COMMAND ----------

use sl1_20200930_v20220529_03

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from contracts where mainproduct is null

-- COMMAND ----------

show tables in SL1_20200630_v20220529_03

-- COMMAND ----------

create table SL1_20200630_v20220529_03.Contracts
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220529_03/SourceData/HarmonizedData/Contracts/*");

-- COMMAND ----------

use SL1_20200630_v20220529_03

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from contracts where mainproduct is null

-- COMMAND ----------

show tables in SL1_20200331_v20220529_03

-- COMMAND ----------

create table SL1_20200331_v20220529_03.Contracts
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_03/SourceData/HarmonizedData/Contracts/*");

-- COMMAND ----------

use SL1_20200331_v20220529_03

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from contracts where mainproduct is null

-- COMMAND ----------

show tables in SL1_20191231_v20220529_03

-- COMMAND ----------

create table SL1_20191231_v20220529_03.Contracts
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_03/SourceData/HarmonizedData/Contracts/*");

-- COMMAND ----------

use SL1_20191231_v20220529_03

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from contracts where mainproduct is null

-- COMMAND ----------

use db_202012_20220621_080921_641; select distinct mainproduct from Contracts

-- COMMAND ----------

Show tables in mireporting

-- COMMAND ----------

select * from mireporting.cashflowsmetrics_1_9
