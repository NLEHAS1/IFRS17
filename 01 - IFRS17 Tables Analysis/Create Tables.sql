-- Databricks notebook source
show tables in mi2022

-- COMMAND ----------

select distinct optionid from mi2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

-- MAGIC %md #20201231.v20220329_01

-- COMMAND ----------

CREATE  table MI2022.cashflows_20201231_v20220329_01
USING orc OPTIONS (path "/mnt/sl2/20201231.v20220329_01/CashFlows/*")


-- COMMAND ----------



-- COMMAND ----------


use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;

-- COMMAND ----------

select count(distinct insurerId) from contracts where mainunit='LOC-NLD'

-- COMMAND ----------

select count(distinct insurerId) from contracts where mainunit='LOC-NLD'

-- COMMAND ----------

select * from contracts where mainunit='LOC-NLD'

-- COMMAND ----------

select CustomerCountry, count(distinct insuredid) as InsuredN from contracts group by CustomerCountry

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202006_20220412_143707_198")

-- COMMAND ----------

show databases

-- COMMAND ----------

use db_201912_20220118_160552_801;

-- COMMAND ----------

show tables in db_2021_09_02_213548_e922369e275771b7d8bb4028e661c3af39e40356;

-- COMMAND ----------

show tables in db_201912_20220118_160552_801

wasbs://data@prdifrs17sl2.blob.core.windows.net/202012_20220413_140845_207
wasbs://data@prdifrs17sl2.blob.core.windows.net/20220413_195153_208


-- COMMAND ----------



wasbs://data@prdifrs17sl2.blob.core.windows.net/202012_20220413_140845_207


-- COMMAND ----------

-- MAGIC %fs ls /mnt/sl2/202012_20220413_140845_207

-- COMMAND ----------

drop table MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

CREATE  table MI2022.cashflows_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/CashFlows/*")


-- COMMAND ----------

select distinct OptionId from MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

CREATE  table MI2022.cashflows_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/CashFlows/*")


-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/CashFlows")

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

desc extended cashflows_201912_20211123_105452_52

-- COMMAND ----------


select count(*) from cashflows_201912_20211123_105452_52

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20211123_105452_52 

-- COMMAND ----------

select count(*) from MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

CREATE  table MI2022.Contracts_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/Contracts/*")


-- COMMAND ----------

CREATE  table MI2022.Contracts_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Contracts/*")


-- COMMAND ----------

select count(*) from MI2022.Contracts_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.Contracts_201912_20220413_133443_206

-- COMMAND ----------

CREATE  table MI2022.Groupings_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/Groupings/*")


-- COMMAND ----------

CREATE  table MI2022.Groupings_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Groupings/*")

-- COMMAND ----------

select count(*) from MI2022.Groupings_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.Groupings_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206")

-- COMMAND ----------

CREATE  table MI2022.DateDistributions_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/DateDistributions/*")


-- COMMAND ----------

select count(*) from MI2022.DateDistributions_201912_20220413_133443_206

-- COMMAND ----------

CREATE  table MI2022.FxRates_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/FxRates/*")

-- COMMAND ----------

select count(*) from MI2022.FxRates_201912_20220413_133443_206

-- COMMAND ----------

CREATE  table MI2022.Entities_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Entities/*")

-- COMMAND ----------

select count(*) from MI2022.Entities_201912_20220413_133443_206

-- COMMAND ----------

CREATE  table MI2022.Hierarchies_201912_20220413_133443_206
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/Hierarchies/*")

-- COMMAND ----------

select count(*) from MI2022.Hierarchies_201912_20220413_133443_206

-- COMMAND ----------

select distinct mainunit from  MI2022.Groupings_202012_20220413_140845_207

-- COMMAND ----------

select * from MI2022.Groupings_202012_20220413_140845_207 where mainunit like'%SPU%'

-- COMMAND ----------

select distinct mainproduct,mainunit from MI2022.Contracts_202012_20220413_140845_207 where mainunit like '%SPU%'

-- COMMAND ----------

select distinct left(mainunit, 3) from MI2022.Contracts_202012_20220413_140845_207

-- COMMAND ----------

MI2022.Groupings2_202012_20220413_140845_207 

-- COMMAND ----------

with CTE as (
  select
    distinct a.ValuationDate,
    b.CalculationEntity,
    b.InsurerId,
    b.InsuredId,
    a.MainUnit,
    a.MainProduct,
    GroupingKey,
    Order
  from
    MI2022.Contracts_202012_20220413_140845_207 a
    left join MI2022.Groupings_202012_20220413_140845_207 b on a.ValuationDate = b.ValuationDate
    And (
      a.mainunit like '%GLB%'
      and left(a.mainunit, 3) = left(b.mainunit, 3)
      and a.mainproduct = b.mainproduct
    )
    OR (
      a.mainunit = b.mainunit
      and a.mainproduct = b.mainproduct
    )
    OR (
      a.mainunit like '%Group'
      and left(a.mainunit, 3) = left(b.mainunit, 3)
    )
    OR (
      a.MainUnit like '%INW%'
      and left(b.mainunit, 3) = left(a.mainunit, 3)
      and SUBSTRING_INDEX(b.mainproduct, "*", -1) = SUBSTRING_INDEX(a.mainproduct, "-", -1) --MainProduct like '%AXL' has no data in Groupings. Check with Amir if XL is equal to AXL
    )
    OR (
      a.mainunit like '%SPU%'
      and 
        (
          left(a.mainunit, 3) = left(b.mainunit, 3)
          and b.mainproduct = a.mainproduct
        )
        or 
          (
            left(b.mainunit, 3) = left(a.mainunit, 3)
            and b.mainproduct = '*'
          )
        
    ) -- mainproduct='SPU-CCPE' isn't available in Contracts table, thus not available in Groupings (You can change the join to  solve this issue)
      -- There are nulls in MainProduct for SPU units because the Contracts table have nulls
      -- the last SPU join isn't correct. The table assign to Groupingkeys for SPU-LP for example
    
    
)
select
  *
from
  CTE


-- COMMAND ----------

select * from MI2022.Groupings_202012_20220413_140845_207  --where mainunit like '%SPU%'

-- COMMAND ----------

select distinct mainproduct,mainunit from MI2022.Contracts_202012_20220413_140845_207
