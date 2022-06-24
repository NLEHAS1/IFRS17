-- Databricks notebook source
show tables in mi2022

-- COMMAND ----------

select distinct OptionId from mi2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from mi2022.tbor_non_ncm_organisations_sl1_20191231

-- COMMAND ----------

select distinct effect_to_dat from mi2022.tbor_non_ncm_organisations_sl1_20191231

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220325_01/SourceData/ReferenceData/Hierarchies")

-- COMMAND ----------

create temp view Entites_20191231_v20220325_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220325_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

select distinct valuationdate from Entites_20191231_v20220325_01

-- COMMAND ----------

create temp view Hierarchies_20191231_v20220325_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220325_01/SourceData/ReferenceData/Hierarchies")

-- COMMAND ----------

select distinct valuationdate from Hierarchies_20191231_v20220325_01

-- COMMAND ----------

create temp view Entites_20200331_v20220323_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220323_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

drop view Hierarchies_20200331_v20220323_01

-- COMMAND ----------

create temp view Hierarchies_20200331_v20220323_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220323_01/SourceData/ReferenceData/Hierarchies")

-- COMMAND ----------

select distinct valuationdate from Entites_20200331_v20220323_01

-- COMMAND ----------

select distinct valuationdate from Hierarchies_20200331_v20220323_01

-- COMMAND ----------

create temp view Entites_20200630_v20220331_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220331_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

create temp view Hierarchies_20200630_v20220331_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220331_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

select distinct valuationdate from Entites_20200630_v20220331_01

-- COMMAND ----------

select distinct valuationdate from Hierarchies_20200630_v20220331_01

-- COMMAND ----------

create temp view Entites_20200930_v20220328_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220328_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

create temp view Hierarchies_20200930_v20220328_01
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220328_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

select distinct valuationdate from Entites_20200930_v20220328_01

-- COMMAND ----------

select distinct valuationdate from Hierarchies_20200930_v20220328_01

-- COMMAND ----------

create temp view Entites_20201231_v20220329_01
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

create temp view Hierarchies_20201231_v20220329_01
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220329_01/SourceData/ReferenceData/Entities")

-- COMMAND ----------

select distinct valuationdate from Entites_20201231_v20220329_01

-- COMMAND ----------

select distinct valuationdate from Hierarchies_20201231_v20220329_01

-- COMMAND ----------


With DateDistributionsTrans as (
  with Datedistributionsint as (
    select
      DateDistributionId,
      DensityId,
      ValuationDate,
      CashFlowDate,
      LossEventDate,
      InvoiceDate,
      ReceivableDueDate,
      case
        when Weight is null then 0
        else Weight
      end as Weight
    from
      db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions
  ),
  Datedistributionsint2 as (
    select
      DateDistributionid,
      ValuationDate,
      Case
        When weight is null then CashflowDate
        when weight = 1 then CashflowDate
        When weight != 1
        and CashflowDate > Valuationdate then 47121231 --> ExpectedCashflow
        When weight != 1
        and CashflowDate <= Valuationdate then 00000000
      End as CustomCashflowDate,
      sum(weight) as weight
    from
      Datedistributionsint
    group by
      DateDistributionid,
      ValuationDate,
      CustomCashflowDate
  )
  select
    DateDistributionid,
    ValuationDate,
    CustomCashflowDate,
    cast(round(weight) as int) as Weight -- Cast the weight as integer will solve the issue of summing the weights
  from
    Datedistributionsint2)
    
    select count(*) from DateDistributionsTrans

-- COMMAND ----------

with DatedistributionsSubset (
select
  DateDistributionid,
  ValuationDate,
  Case
  When weight is null  then CashflowDate
  when weight=1 then CashflowDate
  When weight!=1 and CashflowDate>Valuationdate then 47121231 --> ExpectedCashflow
  When weight!=1 and CashflowDate <= Valuationdate then 00000000
  End as CustomCashflowDate,
  sum(COALESCE(weight,0)) as weight
from
  db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions
group by
DateDistributionid,
ValuationDate,
CustomCashflowDate)

select * from DatedistributionsSubset where CustomCashflowDate > ValuationDate and CustomCashflowDate<47121231

-- COMMAND ----------


With DateDistributionsTrans as (
  with Datedistributionsint as (
    select
      DateDistributionId,
      DensityId,
      ValuationDate,
      CashFlowDate,
      LossEventDate,
      InvoiceDate,
      ReceivableDueDate,
      case
        when Weight is null then 0
        else Weight
      end as Weight
    from
      db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions
  ),
  Datedistributionsint2 as (
    select
      DateDistributionid,
      ValuationDate,
      Case
        When weight is null then CashflowDate
        when weight = 1 then CashflowDate
        When weight != 1
        and CashflowDate > Valuationdate then 47121231 --> ExpectedCashflow
        When weight != 1
        and CashflowDate <= Valuationdate then 00000000
      End as CustomCashflowDate,
      sum(weight) as weight
    from
      Datedistributionsint
    group by
      DateDistributionid,
      ValuationDate,
      CustomCashflowDate
  )
  select
    DateDistributionid,
    ValuationDate,
    CustomCashflowDate,
    cast(round(weight) as int) as Weight -- Cast the weight as integer will solve the issue of summing the weights
  from
    Datedistributionsint2)
    
    select * from DateDistributionsTrans where CustomCashflowDate > ValuationDate and CustomCashflowDate<47121231
