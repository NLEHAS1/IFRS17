-- Databricks notebook source
-- MAGIC %md #Database Analysis

-- COMMAND ----------

-- MAGIC %md The versions of the database delivered to SAS as data input for IFRS17 Calculation Engine

-- COMMAND ----------

show databases

-- COMMAND ----------

-- MAGIC %md Using the last version of the database

-- COMMAND ----------

use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;
show tables

-- COMMAND ----------

-- MAGIC %md #Datedistributions Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for Datedistributions table such as data type, row numbers, size ..etc

-- COMMAND ----------

ANALYZE TABLE MI2022.DateDistributions_201912_20220413_133443_206 COMPUTE STATISTICS ;
DESC EXTENDED MI2022.DateDistributions_201912_20220413_133443_206;

-- COMMAND ----------

-- MAGIC %md 100 rows exctract from the Datedistributions table

-- COMMAND ----------

select * from Datedistributions limit 100

-- COMMAND ----------

-- MAGIC %py
-- MAGIC DatedistributionsPy = sqlContext.sql('Select * from MI2022.datedistributions_202012_20220528_010617_253')

-- COMMAND ----------

-- MAGIC %md Summary statistics for Datedistributions table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC DatedistributionsPy.summary().show()

-- COMMAND ----------

-- MAGIC %md Number of null values in the Datedistributions table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC DatedistributionsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in DatedistributionsPy.columns]).show()

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where weight is null

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in Datedistributions table. Please note that CashflowDate uniqe values is equal to InvoiceDate unique values which means that they are at the same level of aggregation.

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT DateDistributionId) AS DateDistributionId,
COUNT(DISTINCT DensityId) AS DensityId,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT CashFlowDate) AS CashFlowDate,
COUNT(DISTINCT LossEventDate) AS LossEventDate,
COUNT(DISTINCT InvoiceDate) AS InvoiceDate,
COUNT(DISTINCT ReceivableDueDate) AS ReceivableDueDate,
COUNT(DISTINCT Weight) AS Weight
--COUNT(DISTINCT OptionId) AS OptionId/ Not available in db_2021_09_07_140300_f5f6795fc7410aad2a0a09b6ccdc091029237cf3
FROM Datedistributions

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##DateDistributionId  

-- COMMAND ----------

-- MAGIC %md The number of unique DateDistributionId in the Datedistribution table

-- COMMAND ----------

select count(distinct DateDistributionId) from datedistributions

-- COMMAND ----------

-- MAGIC %md #Referentioal Integrity

-- COMMAND ----------

-- MAGIC %md Validate DatedistributionId in the DateDistributions and Cashflows tables

-- COMMAND ----------

select DateDistributionId from MI2022.datedistributions_202012_20220528_010617_253
except
select DateDistributionId from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

select DateDistributionId from MI2022.cashflows_202012_20220528_010617_253
except
select DateDistributionId from MI2022.datedistributions_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md ##ValuationDate 

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from MI2022.datedistributions_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md #Weight (Not Key)

-- COMMAND ----------

-- MAGIC %md The output below means that all datedistributionid with weight equal to 1 has unique records for datedistributionid

-- COMMAND ----------

SELECT datedistributionid, COUNT(datedistributionid)
FROM MI2022.datedistributions_202012_20220528_010617_253
where weight=1
GROUP BY datedistributionid
HAVING COUNT(datedistributionid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The number of DateDistrbutionIds with weight equal to one which have unique records

-- COMMAND ----------

select count(distinct datedistributionid) from datedistributions where weight=1

-- COMMAND ----------

-- MAGIC %md The code below shows the number of records in datedistributions where weight is equal to zero or have negative sign or greater than 1

-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220528_010617_249 where weight<=0 or weight>1

-- COMMAND ----------

-- MAGIC %md The code below show us that all Actual cashflows has weight =1 

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where cashflowdate <= valuationdate and weight != 1

-- COMMAND ----------

-- MAGIC %md The codes below show us that Expected cashflows can have weight =1 and weight !=1

-- COMMAND ----------

select count(*) from datedistributions where cashflowdate > valuationdate and weight != 1

-- COMMAND ----------

select count(*) from datedistributions where cashflowdate > valuationdate and weight = 1

-- COMMAND ----------

select count(*)from MI2022.datedistributions_202012_20220528_010617_253 where weight=0

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where weight=0

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220528_010617_253 where weight<0

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where weight<0

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220528_010617_253 where weight>1

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where weight>1

-- COMMAND ----------

-- MAGIC %md ###CashflowDate

-- COMMAND ----------

-- MAGIC %md The code below show us that we have cashflow date from 1966!

-- COMMAND ----------

select distinct cashflowdate from MI2022.DateDistributions_201912_20220413_133443_206 order by 1

-- COMMAND ----------

-- MAGIC %md The number of distinct cashflow date

-- COMMAND ----------

select count(distinct cashflowdate) from datedistributions

-- COMMAND ----------

-- MAGIC %md In the code below we are grouping the Cashflow Date per month

-- COMMAND ----------

select substr(cashflowdate,1,6) as cashflowdate from datedistributions group by substr(cashflowdate,1,6) order by 1

-- COMMAND ----------

-- MAGIC %md New DateDistributions view 

-- COMMAND ----------

drop view Datedistributions2

-- COMMAND ----------

drop view Datedistributions2

-- COMMAND ----------

desc Datedistributions

-- COMMAND ----------

create view Datedistributionsintials as
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
from Datedistributions

-- COMMAND ----------

create view Datedistributions2 as
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
  Datedistributionsintials
group by
DateDistributionid,
ValuationDate,
CustomCashflowDate

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select count(*) from datedistributions2

-- COMMAND ----------

-- MAGIC %md The code below show us that all DateDistributionid found in datedistributions2

-- COMMAND ----------

select distinct DateDistributionid from datedistributions2
except 
select distinct DateDistributionid from datedistributions

-- COMMAND ----------

select distinct DateDistributionid from datedistributions
except 
select distinct DateDistributionid from datedistributions2

-- COMMAND ----------

-- MAGIC %md The code below show us that some DateDistributionid after transformations they have two records as some of DateDistributionid have expected and actuals

-- COMMAND ----------

select DateDistributionid,count(*) from Datedistributions2
group by DateDistributionid
having count(*)>1

-- COMMAND ----------

create view datetemp as 
select DateDistributionid,count(*) from Datedistributions2
group by DateDistributionid
having count(*)>1

-- COMMAND ----------

select count(*) from datetemp

-- COMMAND ----------

select * from datedistributions2 where datedistributionid='-2141610057'

-- COMMAND ----------

select * from datedistributions where datedistributionid='-2141610057'

-- COMMAND ----------

select * from cashflows  where datedistributionid='-2141610057'

-- COMMAND ----------

select * from cashflows a left join   datedistributions2 b on (a.datedistributionid=b.datedistributionid) where a.datedistributionid='-2141610057' and cashflowid='32332666#SmallRecoveries_d81231f1d4d1f0dd0dd69c384cfcd9b8#85899370454'

-- COMMAND ----------

-- MAGIC %md #PKs Validation

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

-- MAGIC %md these two columns "Primary keys" aren't uniquely identifying a row

-- COMMAND ----------

select
  DateDistributionId,
  ValuationDate,
  ClaimReceivedDate,
  CashFlowDate,
  LossEventDate,
  InvoiceDate,
  ReceivableDueDate,
  count(*)
from
  MI2022.datedistributions_202012_20220528_010617_253
group by
 DateDistributionId,
  ValuationDate,
  ClaimReceivedDate,
  CashFlowDate,
  LossEventDate,
  InvoiceDate,
  ReceivableDueDate
having
  count(*) > 1
order by
  8 desc

-- COMMAND ----------

-- MAGIC %md the code below show suggestion for primary keys but I'm not sure if will stand for future releases

-- COMMAND ----------

select * from datedistributions

-- COMMAND ----------

select 
DateDistributionId,
CashFlowDate,
LossEventDate,
InvoiceDate,
ReceivableDueDate, 
count(*)
from datedistributions
group by 
DateDistributionId,
CashFlowDate,
LossEventDate,
InvoiceDate,
ReceivableDueDate
having count(*)>1
order by 3 desc

-- COMMAND ----------

-- MAGIC %md PK Validation for Datedistributions2

-- COMMAND ----------

select DateDistributionId,ValuationDate,count(*) from datedistributions2 group by DateDistributionId,ValuationDate having count(*)>1 order by 3 desc

-- COMMAND ----------

-- MAGIC %md The following primary keys can work for DateDistributions2

-- COMMAND ----------

select 
DateDistributionId,
CustomCashflowDate,
count(*)
from datedistributions2
group by 
DateDistributionId,
CustomCashflowDate
having count(*)>1
order by 3 desc

-- COMMAND ----------

select * from datedistributions2 where weight=0

-- COMMAND ----------

select  from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions2 

-- COMMAND ----------

select  datedistributionid, count(*) from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions2 group by datedistributionid having count(*)>1

-- COMMAND ----------

select  datedistributionid,CustomCashflowDate, count(*) from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions2 group by datedistributionid,CustomCashflowDate having count(*)>1

-- COMMAND ----------

desc extended db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions2 

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions where weight is null

-- COMMAND ----------

select
  b.datedistributionid
  ,count(*)
from
  db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.cashflows a
  left join db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions b on a.datedistributionid = b.datedistributionid
where a.datedistributionid in (select distinct datedistributionid from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions where weight is null)
group by b.datedistributionid
having count(*)=1

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions2 where datedistributionid=-2074631209

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions where datedistributionid=-2014285709

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.cashflows where datedistributionid=-2074631209 

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions where weight!=1

-- COMMAND ----------

select * from (select
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
CustomCashflowDate) where datedistributionid=-2147076628


-- COMMAND ----------

-- MAGIC %md #The Final view to cerate DateDistributionsSubset

-- COMMAND ----------

-- MAGIC %md this view can deal with nulls and aggregates weights for distributed cashflows

-- COMMAND ----------

select distinct weight from (
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

-- COMMAND ----------

-- MAGIC %md the code below get rid of the non 0,1 weights

-- COMMAND ----------

--Count=8750394

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
    Datedistributions
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
  Datedistributionsint2 
