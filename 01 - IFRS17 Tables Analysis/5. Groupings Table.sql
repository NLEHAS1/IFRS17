-- Databricks notebook source
-- MAGIC %md #Database Analysis

-- COMMAND ----------

-- MAGIC %md The versions of the database delivered to SAS as data input for IFRS17 Calculation Engine

-- COMMAND ----------

-- DBTITLE 0,The versions of the database delivered to SAS as data input for IFRS17 Calculation Engine
show databases

-- COMMAND ----------

-- MAGIC %md Using the last version of the database

-- COMMAND ----------

use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;
show tables

-- COMMAND ----------

-- MAGIC %md #Groupings Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for the Groupings table such as data type, row numbers, size ..etc

-- COMMAND ----------

ANALYZE TABLE MI2022.Groupings_201912_20220413_133443_206 COMPUTE STATISTICS ;
DESC EXTENDED MI2022.Groupings_201912_20220413_133443_206;

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC library(SparkR)
-- MAGIC GroupingsR <- sql("Select * from Groupings")
-- MAGIC str(GroupingsR)

-- COMMAND ----------

-- MAGIC %md Show Groupings table. As we can see the Order attribute is unique for each combination of mainunit and mainproduct

-- COMMAND ----------

select * from MI2022.Groupings_201912_20220413_133443_206 

-- COMMAND ----------

-- MAGIC %md Summary statistics for Groupings table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC GroupingsPy = sqlContext.sql('Select * from Groupings')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dfe= GroupingsPy.summary()
-- MAGIC display(dfe)

-- COMMAND ----------

-- MAGIC %md Number of null values in the Groupings table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC gz= GroupingsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in GroupingsPy.columns])
-- MAGIC display(gz)

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in Groupings table

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT CalculationEntity) AS CalculationEntity,
COUNT(DISTINCT InsurerId) AS InsurerId,
COUNT(DISTINCT InsuredId) AS InsuredId,
COUNT(DISTINCT MainUnit) AS MainUnit,
COUNT(DISTINCT MainProduct) AS MainProduct,
COUNT(DISTINCT GroupingKey) AS GroupingKey,
COUNT(DISTINCT Order) AS Order
--COUNT(DISTINCT OptionId) AS OptionId/ Not available in db_2021_09_07_140300_f5f6795fc7410aad2a0a09b6ccdc091029237cf3
FROM Groupings

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##ValuationDate 

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from Groupings

-- COMMAND ----------

-- MAGIC %md ##Order

-- COMMAND ----------

-- MAGIC %md The code table show us the distinct values of Order 

-- COMMAND ----------

select distinct order from Groupings order by 1 asc

-- COMMAND ----------

-- MAGIC %md ###MainUnit & MainProduct (Not Keys)

-- COMMAND ----------

-- MAGIC %md The table below show the repeated records under each Mainunit in the Groupings table

-- COMMAND ----------

select mainunit, count(mainunit)
from groupings
Group by mainunit
having count(mainunit)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md Example of repeated MainUnit record where mainunit equal to LOC-DEU

-- COMMAND ----------

select * from groupings where mainunit='LOC-DEU'

-- COMMAND ----------

-- MAGIC %md The table below show the repeated records under each MainProduct in the Groupings table

-- COMMAND ----------

select mainproduct, count(mainproduct)
from groupings
Group by mainproduct
having count(mainproduct)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md Example of repeated Mainproduct records where Mainproduct equal to CI-ST

-- COMMAND ----------

select * from groupings where mainproduct='CI-ST'

-- COMMAND ----------

-- MAGIC %md ##Check if Mainunit and MainProduct in Groupings and Contracts are equal

-- COMMAND ----------

-- MAGIC %md MainUnit

-- COMMAND ----------

select distinct MainUnit from contracts where MainUnit not in (select MainUnit from groupings)

-- COMMAND ----------

select distinct MainUnit from groupings where MainUnit not in (select distinct MainUnit from contracts)

-- COMMAND ----------

-- MAGIC %md MainProduct

-- COMMAND ----------

select distinct MainProduct from contracts where MainProduct not in (select MainProduct from groupings)

-- COMMAND ----------

select distinct MainProduct from groupings where MainProduct not in (select distinct MainProduct from contracts)

-- COMMAND ----------

-- MAGIC %md #PK Validation

-- COMMAND ----------

select valuationdate, order, count(*) from groupings group by valuationdate, order having count(*)>1

-- COMMAND ----------

select * from groupings

-- COMMAND ----------

select * from groupings2

-- COMMAND ----------

-- MAGIC %md As you can see the original primary keys won't work for Groupings2

-- COMMAND ----------

select valuationdate, order, count(*) from groupings2 group by valuationdate, order having count(*)>1

-- COMMAND ----------

-- MAGIC %md A proposed primarykeys are MainUnit and MainProduct

-- COMMAND ----------

select MainProduct, mainunit,count(*) from groupings2 group by MainProduct,mainunit having count(*)>1

-- COMMAND ----------

select 
ValuationDate,EffectFromDate,FromCurrency,ToCurrency,ConversionType, count(*)
from fxrates
group by ValuationDate,EffectFromDate,FromCurrency,ToCurrency,ConversionType
having count(*)>1
order by 6 desc

-- COMMAND ----------

-- MAGIC %md #Groupings 2

-- COMMAND ----------

create view db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.Groupings2 as
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
  contracts a
  left join groupings b on (a.ValuationDate = b.ValuationDate)
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

-- COMMAND ----------

select count(*) from groupings2 group by MainProduct having count(*)>1

-- COMMAND ----------

select * from 

-- COMMAND ----------

-- MAGIC %md #TempGroupings

-- COMMAND ----------

select 
distinct
ValuationDate,
Mainunit,
MainProduct,
case
When MainProduct in ('SPU-LP','SPU-CEND','SPU-CCPE') then 'SPU-POLITICAL'
When MainProduct like '%SPU%' and MainProduct not in ('SPU-LP','SPU-CEND','SPU-CCPE') then 'SPU'
When left(MainProduct, 3)='GLB' and MainProduct='NonInsurance' then 'NonInsurance_GLB'
when Mainunit like '%LOC%' and MainProduct='NonInsurance' then Concat(MainProduct,'_',Mainunit)  
When Mainunit like '%LOC%' and MainUnit not in ('LOC-SGP-OFF' ,'LOC-SGP-ON')  and MainProduct='CI-ST' then Concat(MainProduct,'_',Mainunit)  
When MainUnit in ('LOC-SGP-OFF' ,'LOC-SGP-ON') and MainProduct='CI-ST' then 'CI-ST_LOC-SGP' 
When MainUnit like '%GLB%' and MainUnit not in ('GLB-SGP-ON','GLB-SGP-OFF') and MainProduct='CI-ST' then 'CI-ST_GLB'
When MainUnit  in ('GLB-SGP-ON','GLB-SGP-OFF') and MainProduct='CI-ST' then 'CI-ST_GLB'
When Mainunit like '%LOC%' and MainProduct='CI-MTB' then Concat(MainProduct,'_',Mainunit) 
When Mainunit like '%GLB%' and MainProduct='CI-MTB' then 'CI-MTB_GLB'
When Mainunit like '%INW%' and MainProduct like '%XL%' then 'InwardReXL'
When Mainunit like 'INW%' and MainProduct like '%VQS%' then 'InwardReQS'
When Mainunit like 'INW%' and MainProduct like '%SP%' then 'InwardReSP'
When Mainunit like 'INW%' and MainProduct like '%SL%' then 'InwardReSL'
When Mainunit like 'INW%' and MainProduct like '%QS%' then 'InwardReQS'
When Mainunit like 'INW%' and MainProduct like '%OC' then 'InwardReOC'
When Mainunit like 'INW%' and MainProduct like '%FAC%' then 'InwardReFac'
When Mainunit like 'INW%' and MainProduct like  '%FAC%' then 'InwardReFac'
When MainUnit='IntraGroup' then 'ReinsuranceIntraGroup'
When MainUnit='ExtraGroup' then 'ReinsuranceExtraGroup'
When MainUnit='BON-ITA' then 'BON-ITA'
When MainUnit='BON-BEL' then 'BON-BEL'
When MainUnit='BON-LUX' then 'BON-LUX'
When MainUnit='BON-SWE' then 'BON-SWE'
When MainUnit='BON-NOR' then 'BON-NOR'
When MainUnit='BON-FIN' then 'BON-FIN'
When MainUnit='BON-DNK' then 'BON-DNK'
When MainUnit='BON-NLD' then 'BON-NLD'
When MainUnit='BON-DEU' then 'BON-DEU'
When MainUnit='BON-FRA' then 'BON-FRA'
When MainUnit='BON-ESP' then 'BON-ESP'
When MainUnit='BON-PRT' then 'BON-PRT'
End as GroupingKey,
case
When MainProduct='SPU-LP' then 100
When MainProduct='SPU-CEND' then 101
When MainProduct='SPU-CCPE' then 102
When MainProduct like '%SPU%' and MainProduct not in ('SPU-LP','SPU-CEND','SPU-CCPE') then 110
When left(MainProduct, 3)='GLB' and MainProduct='NonInsurance' then 704
when Mainunit='LOC-NLD' and MainProduct='NonInsurance' then 703
when Mainunit='LOC-FRA' and MainProduct='NonInsurance' then 702
when Mainunit='LOC-DEU' and MainProduct='NonInsurance'then 701
when Mainunit='LOC-ARE' and MainProduct='CI-ST' then	13
when Mainunit='LOC-AUS' and MainProduct='CI-ST' then	14
when Mainunit='LOC-AUT' and MainProduct='CI-ST' then	15
when Mainunit='LOC-BEL' and MainProduct='CI-ST' then	16
when Mainunit='LOC-BGR' and MainProduct='CI-ST' then	17
when Mainunit='LOC-CAN' and MainProduct='CI-ST' then	18
when Mainunit='LOC-CHE' and MainProduct='CI-ST' then	19
when Mainunit='LOC-CHN' and MainProduct='CI-ST' then	20
when Mainunit='LOC-CZE' and MainProduct='CI-ST' then	21
when Mainunit='LOC-DEU' and MainProduct='CI-ST' then	22
when Mainunit='LOC-DNK' and MainProduct='CI-ST' then	23
when Mainunit='LOC-ESP' and MainProduct='CI-ST' then	24
when Mainunit='LOC-FIN' and MainProduct='CI-ST' then	25
when Mainunit='LOC-FRA' and MainProduct='CI-ST' then	26
when Mainunit='LOC-GBR' and MainProduct='CI-ST' then	27
when Mainunit='LOC-GRC' and MainProduct='CI-ST' then	28
when Mainunit='LOC-HKG' and MainProduct='CI-ST' then	29
when Mainunit='LOC-HUN' and MainProduct='CI-ST' then	30
when Mainunit='LOC-IND' and MainProduct='CI-ST' then	31
when Mainunit='LOC-IRL' and MainProduct='CI-ST' then	32
when Mainunit='LOC-ITA' and MainProduct='CI-ST' then	33
when Mainunit='LOC-JPN' and MainProduct='CI-ST' then	34
when Mainunit='LOC-LUX' and MainProduct='CI-ST' then	35
when Mainunit='LOC-MEX' and MainProduct='CI-ST' then	36
when Mainunit='LOC-NLD' and MainProduct='CI-ST' then	37
when Mainunit='LOC-NOR' and MainProduct='CI-ST' then	38
when Mainunit='LOC-NZL' and MainProduct='CI-ST' then	39
when Mainunit='LOC-POL' and MainProduct='CI-ST' then	40
when Mainunit='LOC-PRT' and MainProduct='CI-ST' then	41
when Mainunit='LOC-ROU' and MainProduct='CI-ST' then	42
when Mainunit='LOC-SGP' and MainProduct='CI-ST' then	43
when Mainunit='LOC-SGP-OFF' and MainProduct='CI-ST' then	44
when Mainunit='LOC-SGP-ON' and MainProduct='CI-ST' then	45
when Mainunit='LOC-SVK' and MainProduct='CI-ST' then	46
when Mainunit='LOC-SWE' and MainProduct='CI-ST' then	47
when Mainunit='LOC-TUR' and MainProduct='CI-ST' then	48
when Mainunit='LOC-TWN' and MainProduct='CI-ST' then	49
when Mainunit='LOC-USA' and MainProduct='CI-ST' then	50
When MainUnit like '%GLB%' and MainUnit not in ('GLB-SGP-ON','GLB-SGP-OFF') and MainProduct='CI-ST' then 4
When MainUnit='GLB-SGP-ON' and MainProduct='CI-ST' then 3
When MainUnit='GLB-SGP-OFF' and MainProduct='CI-ST' then 2
when Mainunit='LOC-AUS' and MainProduct='CI-MTB' then	5
when Mainunit='LOC-AUT' and MainProduct='CI-MTB' then	6
when Mainunit='LOC-CHE' and MainProduct='CI-MTB' then	7
when Mainunit='LOC-DEU' and MainProduct='CI-MTB' then	8
when Mainunit='LOC-DNK' and MainProduct='CI-MTB' then	9
when Mainunit='LOC-GBR' and MainProduct='CI-MTB' then	10
when Mainunit='LOC-ITA' and MainProduct='CI-MTB' then	11
when Mainunit='LOC-NLD' and MainProduct='CI-MTB' then	12
When Mainunit like '%GLB%' and MainProduct='CI-MTB' then 1
When Mainunit like '%INW%' and MainProduct  like '%XL%' then 402
When Mainunit like '%INW%' and MainProduct like '%VQS%' then 401
When Mainunit like '%INW%' and MainProduct like '%SP%' then  404
When Mainunit like '%INW%' and MainProduct like '%SL%' then 403
When Mainunit like '%INW%' and MainProduct like '%QS%' then 400
When Mainunit like '%INW%' and MainProduct like '%OC' then 407
When Mainunit like '%INW%' and MainProduct like '%FACX%' then 406
When Mainunit like '%INW%' and MainProduct like '%FAC%' then 405
When MainUnit='IntraGroup' then 801
When MainUnit='ExtraGroup' then 800
When MainUnit='BON-ITA' then 		300
When MainUnit='BON-BEL' then		301
When MainUnit='BON-LUX' then		302
When MainUnit='BON-SWE' then		303
When MainUnit='BON-NOR' then		304
When MainUnit='BON-FIN' then		305
When MainUnit='BON-DNK' then		306
When MainUnit='BON-NLD' then		307
When MainUnit='BON-DEU' then		308
When MainUnit='BON-FRA' then		309
When MainUnit='BON-ESP' then		311
When MainUnit='BON-PRT' then		312
End as Order
from MI2022.Contracts_201912_20220413_133443_206




-- COMMAND ----------

select distinct mainunit,mainproduct from MI2022.Contracts_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.Contracts_201912_20220413_133443_206 where mainunit='LOC-KEN'

-- COMMAND ----------

select * from MI2022.Groupings_201912_20220413_133443_206 where mainunit='LOC-KEN'
