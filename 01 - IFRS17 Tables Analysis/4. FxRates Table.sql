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
Show tables

-- COMMAND ----------

-- MAGIC %md #FxRates Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for FxRates table such as data type, row numbers, size ..etc

-- COMMAND ----------

ANALYZE TABLE FxRates COMPUTE STATISTICS ;
DESC EXTENDED FxRates;

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC library(SparkR)
-- MAGIC FxRatesR <- sql("Select * from FxRates")
-- MAGIC str(FxRatesR)

-- COMMAND ----------

-- MAGIC %md 100 rows exctract from the FxRates table

-- COMMAND ----------

select * from fxrates limit 100

-- COMMAND ----------

select distinct ConversionType from fxrates

-- COMMAND ----------

select distinct EffectFromDate from fxrates order by EffectFromDate asc

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

-- MAGIC %md Summary statistics for FxRates table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC FxRatesPy = sqlContext.sql('Select * from MI2022.fxrates_202012_20220528_010617_253')

-- COMMAND ----------

-- MAGIC %py FxRatesPy.summary().show()

-- COMMAND ----------

-- MAGIC %md Number of null values in the FxRates table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC FxRatesPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in FxRatesPy.columns]).show()

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in FxRates table

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT EffectFromDate) AS EffectFromDate,
COUNT(DISTINCT EffectToDate) AS EffectToDate,
COUNT(DISTINCT CalculationEntity) AS CalculationEntity,
COUNT(DISTINCT FromCurrency) AS FromCurrency,
COUNT(DISTINCT ToCurrency) AS ToCurrency,
COUNT (DISTINCT ConversionRate) AS ConversionRate,
COUNT(DISTINCT ConversionType) AS ConversionType,
COUNT(DISTINCT DataSource) AS DataSource,
COUNT(DISTINCT Context) AS Context
--COUNT(DISTINCT OptionId) AS OptionId
FROM FxRates

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##ConversionType

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of ConversionType

-- COMMAND ----------

select distinct ConversionType from MI2022.fxrates_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md ##ValuationDate

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from MI2022.fxrates_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md ##FromCurrency

-- COMMAND ----------

-- MAGIC %md The code below show us that all transactoion currencies conversoin rate are available in FxRates

-- COMMAND ----------

select
  distinct Transactioncurrency
from
  MI2022.cashflows_202012_20220528_010617_253 a left anti
  join MI2022.fxrates_202012_20220528_010617_253 b on (
    a.TransactionCurrency = b.FromCurrency
    and tocurrency = 'EUR'
  )

-- COMMAND ----------

select * from MI2022.fxrates_202012_20220528_010617_253 where fromcurrency='VEB'

-- COMMAND ----------

-- MAGIC %md Check if transaction currencies in the Cashflows have exchange rate to euro

-- COMMAND ----------

select distinct transactioncurrency from MI2022.cashflows_201912_20220413_133443_206
except
select distinct fromcurrency from MI2022.FxRates_201912_20220413_133443_206 where tocurrency='EUR'

-- COMMAND ----------

-- MAGIC %md Check if transaction currencies in the cashlows have exchange rate at valuationdate

-- COMMAND ----------

select distinct TransactionCurrency from MI2022.cashflows_201912_20220413_133443_206 a left join MI2022.datedistributions_201912_20220413_133443_206 b on a.datedistributionid=b.datedistributionid   where cashflowdate > 20191231
except
select distinct fromcurrency from MI2022.FxRates_201912_20220413_133443_206 where tocurrency='EUR' and effectfromdate=20191231

-- COMMAND ----------

select * from MI2022.FxRates_201912_20220413_133443_206 order by effectfromdate asc  

-- COMMAND ----------

select distinct cashflowdate from MI2022.datedistributions_201912_20220413_133443_206 order by cashflowdate asc  

-- COMMAND ----------

select * from MI2022.FxRates_201912_20220413_133443_206 where tocurrency='EUR' and fromcurrency='VEB'

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of FromCurrency

-- COMMAND ----------

select distinct FromCurrency from MI2022.FxRates_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md The table below shows the number of records attached to Currencies which means that FxRates table include ConversoinRate across multiple periods

-- COMMAND ----------

SELECT FromCurrency, COUNT(FromCurrency)
FROM FxRates
GROUP BY FromCurrency
HAVING COUNT(FromCurrency)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The code below show no output because there's no conversion rate from Euro to Euro which equal one. 

-- COMMAND ----------

select * from MI2022.FxRates_201912_20220413_133443_206 where tocurrency='EUR' and fromcurrency='EUR'

-- COMMAND ----------

-- MAGIC %md ##ToCurrency

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of ToCurrency attribute

-- COMMAND ----------

select distinct ToCurrency from FxRates

-- COMMAND ----------

-- MAGIC %md ##EffectFromDate

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of EffectToDate attribute

-- COMMAND ----------

select distinct EffectFromDate from MI2022.FxRates_201912_20220413_133443_206 order by EffectFromDate asc

-- COMMAND ----------

-- MAGIC %md The code below show that EffectFromDate is equal to EffectToDate or all records in FxRates table

-- COMMAND ----------

select * from MI2022.FxRates_201912_20220413_133443_206 where EffectFromDate <> EffectToDate

-- COMMAND ----------

-- MAGIC %md The code below show us that all transactioncurrency have valuationdate=effectfromdate

-- COMMAND ----------

select transactioncurrency from cashflows
except 
select fromcurrency from fxrates where valuationdate=effectfromdate

-- COMMAND ----------

select distinct cashflowdate from datedistributions 
except
select effectfromdate from fxrates

order by cashflowdate asc

-- COMMAND ----------

create view temp as
select distinct cashflowdate from datedistributions 
except
select effectfromdate from fxrates

order by cashflowdate asc

-- COMMAND ----------

select * from db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.fxrates order by effectfromdate asc

-- COMMAND ----------

select distinct cashflowdate from DateDistributions order by cashflowdate asc

-- COMMAND ----------

select * from fxrates where effectfromdate=19810904

-- COMMAND ----------

select * from datedistributions where cashflowdate=19840705


-- COMMAND ----------

select * from cashflows where datedistributionid='-1997618044'

-- COMMAND ----------

-- MAGIC %md The result below tell us that there transactioncurrencies not equal to Euro have no conversion rate based on that effectfromdate start at 19990101 and these transactions have dates before that

-- COMMAND ----------

select distinct TransactionCurrency from cashflows a left join datedistributions b on a.datedistributionid=b.datedistributionid where cashflowdate in (select distinct cashflowdate from temp) and TransactionCurrency!='EUR'

-- COMMAND ----------

select
  distinct a.datedistributionid,
  TransactionCurrency,
  cashflowdate
from
  cashflows a
  left join datedistributions b on a.datedistributionid = b.datedistributionid
where
  cashflowdate < 19990101
  and TransactionCurrency != 'EUR'
order by
  cashflowdate asc

-- COMMAND ----------

select * from fxrates where effectfromdate=19970108

-- COMMAND ----------

select * from datedistributions where datedistributionid=216731060

-- COMMAND ----------

select * from cashflows where datedistributionid=216731060

-- COMMAND ----------

-- MAGIC %md There are 7 currencies that don't have conversion rates equal to valuation date. Robert gave the following explanation: ZWD (not current, Zimbabwean Dollar - now ZWN), VEF (not current, Venuzualan Bolivar - now VES), USS (not current, US Dollar same day rate), STD (Sao Tome and Principe Dobra, not current, replace by STN), ZMK (Zambian Kwacha, not current, replaced by ZMW), XOF (Not sure XFO is not current and it the Gold Franc), and MZM (Mozambician Metical, not current, replaced by MZN).

-- COMMAND ----------

select distinct FromCurrency from fxrates where FromCurrency not in (select distinct FromCurrency from fxrates where EffectFromDate= ValuationDate)

-- COMMAND ----------

-- MAGIC %md #CalculationEntity

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of CalculationEntity (Only wildcard for now)

-- COMMAND ----------

Select distinct CalculationEntity from FxRates

-- COMMAND ----------

-- MAGIC %md #PK Validation

-- COMMAND ----------

select 
ValuationDate,EffectFromDate,CalculationEntity,FromCurrency,ToCurrency,ConversionType, count(*)
from MI2022.fxrates_201912_20220522_180045_173
group by ValuationDate,EffectFromDate,CalculationEntity,FromCurrency,ToCurrency,ConversionType
having count(*)>1
order by 7 desc

-- COMMAND ----------

select 
ValuationDate,EffectFromDate,FromCurrency, count(*)
from fxrates
group by ValuationDate,EffectFromDate,FromCurrency
having count(*)>1
order by 4 desc

-- COMMAND ----------

select EntityId,HierarchyId, count(*) from Hierarchies group by EntityId,HierarchyId having count(*)>1 order by 2 desc

-- COMMAND ----------

select * from Cashflows a left join datedistributions b on a.datedistributionid=b.datedistributionid

-- COMMAND ----------

select * from fxrates where fromcurrency='USD' and ToCurrency='EUR'

-- COMMAND ----------

select * from MI2022.fxrates_202003_20220522_180045_174 where EffectFromDate!=EffectToDate
