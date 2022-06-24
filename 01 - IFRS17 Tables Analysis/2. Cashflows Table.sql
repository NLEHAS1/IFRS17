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

desc cashflows

-- COMMAND ----------

select distinct FutureStateProbability from cashflows

-- COMMAND ----------

select count(*) from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.cashflows

-- COMMAND ----------

-- MAGIC %md #Cashflows Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for the Cashflows table such as data type, row numbers, size ..etc

-- COMMAND ----------

ANALYZE TABLE MI2022.cashflows_201912_20220413_133443_206 COMPUTE STATISTICS ;
DESC EXTENDED MI2022.cashflows_201912_20220413_133443_206;

-- COMMAND ----------

-- MAGIC %md 100 rows exctract from the Cashflows table

-- COMMAND ----------

select * from MI2022.cashflows_202003_20220412_081208_191 limit 100;

-- COMMAND ----------

-- MAGIC %md Summary statistics for Cashflows table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC CashflowsPy = sqlContext.sql('Select * from MI2022.cashflows_202012_20220528_010617_253')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dfee=CashflowsPy.summary()
-- MAGIC display(dfee)

-- COMMAND ----------

-- MAGIC %md Number of null values in the Cashflows table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC cashnull=CashflowsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in CashflowsPy.columns])
-- MAGIC display(cashnull)

-- COMMAND ----------

select * from MI2022.cashflows_202012_20220528_010617_253 where transactioncurrency is null

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in Cashflows table

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT FutureStateId) AS FutureStateId,
COUNT(DISTINCT FutureStateProbability) AS FutureStateProbability,
COUNT(DISTINCT ContractId) AS ContractId,
COUNT(DISTINCT CashFlowId) AS CashFlowId,
COUNT(DISTINCT CashFlowType) AS CashFlowType,
COUNT(DISTINCT CashFlowSubType) AS CashFlowSubType,
COUNT(DISTINCT FromPartyId) AS FromPartyId,
COUNT(DISTINCT ToPartyId) AS ToPartyId,
COUNT(DISTINCT RiskCounterPartyId) AS RiskCounterPartyId,
COUNT(DISTINCT CountryOfRisk) AS CountryOfRisk,
COUNT(DISTINCT DataSource) AS DataSource,
COUNT(DISTINCT ModelId) AS ModelId,
COUNT(DISTINCT InvoiceId) AS InvoiceId,
COUNT(DISTINCT ClaimId) AS ClaimId,
COUNT(DISTINCT FlowSequenceId) AS FlowSequenceId,
COUNT(DISTINCT DateDistributionId) AS DateDistributionId,
COUNT(DISTINCT RiskCurrency) AS RiskCurrency,
COUNT(DISTINCT TransactionCurrency) AS TransactionCurrency,
COUNT(DISTINCT Amount) AS Amount,
COUNT(DISTINCT CounterpartyDefaultAdjustment) AS CounterpartyDefaultAdjustment
--COUNT(DISTINCT OptionId) AS OptionId OptionId isn't available anymore
FROM Cashflows

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##ValuationDate

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md ##FutureStateId 

-- COMMAND ----------

-- MAGIC %md The table below show us the distinct values of FutureStateId

-- COMMAND ----------

select distinct FutureStateGroupId from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md ##FlowSequenceId  

-- COMMAND ----------

-- MAGIC %md The table below below show us the distinct values of FlowSequenceId

-- COMMAND ----------

select distinct FlowSequenceId from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md ##CashFlowId

-- COMMAND ----------

-- MAGIC %md The number of distinct values of CashflowID

-- COMMAND ----------

-- DBTITLE 0,The number of distinct values of CashflowID
select count(distinct cashflowid) from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md The code below shows if the cashflow id is unique accross cashflows table

-- COMMAND ----------

SELECT cashflowid, COUNT(cashflowid)
FROM MI2022.cashflows_201912_20220413_133443_206
GROUP BY cashflowid
HAVING COUNT(cashflowid)>1
order by 2 asc

-- COMMAND ----------

-- MAGIC %md The output below show us if the a cashflow id has multiple contractid attachet to it.

-- COMMAND ----------

select distinct cashflowid, count(distinct contractid)
from MI2022.cashflows_201912_20220413_133443_206
group by cashflowid
having count(distinct contractid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The output below show us if a contract id has multiple cashflowid attached to it

-- COMMAND ----------

select distinct contractid, count(distinct cashflowid)
from mi2022.cashflows_202012_20220413_140845_207
group by contractid
having count(distinct cashflowid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One CashflowID can have multiple policyid attached to it

-- COMMAND ----------

select distinct cashflowid, count(distinct policyid)
from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid)
group by cashflowid
having count(distinct policyid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The code below shows that Cashflows with FlowSequenceId=0 has only one record

-- COMMAND ----------

SELECT cashflowid, COUNT(cashflowid)
FROM MI2022.cashflows_202012_20220528_010617_253
where FlowSequenceId=0
GROUP BY cashflowid
HAVING COUNT(cashflowid)>1 
order by 2 asc

-- COMMAND ----------

-- MAGIC %md #Cashflow Types and SubTypes (Not Keys)

-- COMMAND ----------

-- MAGIC %md The distinct values of CashflowType and CashflowSubType

-- COMMAND ----------

select distinct cashflowtype, cashflowsubtype from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md #Date Sources (Not Key)

-- COMMAND ----------

-- MAGIC %md The distinct values of DataSource column

-- COMMAND ----------

select distinct datasource from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md ##DateDistributionId (not Key)

-- COMMAND ----------

-- MAGIC %md The output below shows if a Cashflowid is attached to multiple DateDistributionId

-- COMMAND ----------

select distinct cashflowid, count(distinct DateDistributionId)
from MI2022.cashflows_202012_20220528_010617_253
group by cashflowid
having count(distinct DateDistributionId)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The output below shows if DateDistributionId is attached to multiple cashflowids

-- COMMAND ----------

select distinct DateDistributionId, count(distinct cashflowid)
from cashflows
group by DateDistributionId
having count(distinct cashflowid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md ##TransactionCurrency (not Key)

-- COMMAND ----------

-- MAGIC %md The code below show us if a cashflowid attached to multiple TransactionCurrencies

-- COMMAND ----------

select distinct cashflowid, count(distinct TransactionCurrency)
from MI2022.cashflows_202012_20220528_010617_253
group by cashflowid
having count(distinct TransactionCurrency)>1
order by 2 desc

-- COMMAND ----------

select distinct transactioncurrency from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md The analysis below is to issue if all TransactionCurrencies have ConversionRate at ValuationDate and CashflowDate

-- COMMAND ----------

select distinct transactioncurrency from cashflows
except
select distinct fromcurrency from fxrates where FromCurrency in (select distinct transactioncurrency from cashflows) and tocurrency='EUR' and EffectFromDate=20201231

-- COMMAND ----------

select distinct transactioncurrency from cashflows
except
select distinct fromcurrency from fxrates where FromCurrency in (select distinct transactioncurrency from cashflows) and tocurrency='EUR' and EffectFromDate > 19990101

-- COMMAND ----------

-- MAGIC %md ##Amount (Not key)

-- COMMAND ----------

-- MAGIC %md The number of Cashflows where the cashflows amount equal to zero

-- COMMAND ----------

select datasource,count(*) from MI2022.cashflows_202012_20220528_010617_253 where amount=0 group by datasource 

-- COMMAND ----------

select count(*) from  MI2022.cashflows_202012_20220528_010617_253 where amount=0 

-- COMMAND ----------

-- MAGIC %md The number of cashflows where cashflows amounts are with negative sign

-- COMMAND ----------

select datasource,count(*) from MI2022.cashflows_202012_20220528_010617_253 where amount<0 group by datasource 

-- COMMAND ----------

-- MAGIC %md example of cashflows with negative amounts

-- COMMAND ----------

select * from MI2022.cashflows_202012_20220528_010617_253 order by amount asc 

-- COMMAND ----------

-- MAGIC %md The table below show us the expected Cashflows where CashFlowDate > ValuationDate

-- COMMAND ----------

--select * from cashflows a left join datedistributions b on a.datedistributionid=b.datedistributionid and a.valuationdate=b.valuationdate
--where CashFlowDate > a.ValuationDate limit 100

-- COMMAND ----------

-- MAGIC %md The query below show us the actual Cashflows where CashFlowDate <= ValuationDate

-- COMMAND ----------

--select * from cashflows a left join datedistributions b on a.datedistributionid=b.datedistributionid and a.valuationdate=b.valuationdate
--where CashFlowDate <= a.ValuationDate limit 100

-- COMMAND ----------

-- MAGIC %md #PKs Validation

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

-- MAGIC %md As we can see from the code belows cashflowid,FlowSequenceId , ValuationDate, and FutureStateId aren't uniquley identifying a row

-- COMMAND ----------

select 
ValuationDate,
cashflowid,
FlowSequenceId ,
ValuationDate,
FutureStateGroupId ,
Count(*)
from MI2022.cashflows_202012_20220528_010617_253
group by 
ValuationDate,
cashflowid,
FlowSequenceId ,
ValuationDate,
FutureStateGroupId 
having Count(*)>1
order by 6 desc

-- COMMAND ----------

-- MAGIC %md The grain of the cashflow transaction is one row for each individual contractid, cashflowid ,FromPartyId,ToPartyId, CashFlowType,CashFlowSubType. The row count show 2 because we have duplicates see below

-- COMMAND ----------

select 
contractid, 
cashflowid ,
FromPartyId,
ToPartyId, 
CashFlowType,
CashFlowSubType,
count(*)
from MI2022.cashflows_201912_20220413_133443_206
group by 
contractid, 
cashflowid ,
FromPartyId,
ToPartyId, 
CashFlowType,
CashFlowSubType
having count(*)>1
order by 7 desc

-- COMMAND ----------

-- MAGIC %md The following code show us the duplicates in Cashflows

-- COMMAND ----------

desc MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

select 
ValuationDate,
FutureStateGroupId ,
FutureStateGroupProbability,
ContractId,
RiskPeriodStartDate,
RiskPeriodEndDate,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
DataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment,
count(*)  
from MI2022.cashflows_202012_20220528_010617_253
group by 
ValuationDate,
FutureStateGroupId ,
FutureStateGroupProbability,
ContractId,
RiskPeriodStartDate,
RiskPeriodEndDate,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
DataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
having count(*)>1
order by 24 desc 

-- COMMAND ----------

-- MAGIC %md As a solution we can do the incremntals based on contractID so if the aggregate cashflows for a contractid has been changed then we add the new rows which has new cashflow amounts (based that the amounts aren't linear then this is would be diffcult) disccus the idea with amir and GB team

-- COMMAND ----------

-- MAGIC %md #Analysis of FromPartyID and ToPartyId

-- COMMAND ----------

-- MAGIC %md ###FromPartyID

-- COMMAND ----------

-- MAGIC %md As we can see from the table below, there are 35 entities fom the entities table found in Cashflows table. 34 entities with true IFRS17CalculationEntity which consist of L3 (203 and 285) and L4 (rest). 1 entity as External.

-- COMMAND ----------

select distinct insurerid,a.EntityId,name,ParentEntityId,IFRS17CalculationEntity from entities a left join hierarchies b on (a.EntityId=b.EntityId) where insurerid in (select distinct frompartyid from cashflows)

-- COMMAND ----------

-- MAGIC %md We notice that the Cashflows "FromParty" transactions at level 3 and 4. An Example:

-- COMMAND ----------

select *  from cashflows where frompartyid in ('15319619','15760182') -- ---> 15319619 L3 and 15760182 LV4

-- COMMAND ----------

-- MAGIC %md ###ToPartyID

-- COMMAND ----------

-- MAGIC %md As we can see from the table below, there are 36 entities fom the entities table found in Cashflows table. 34 entities with true IFRS17CalculationEntity which consist of L3 (203 and 285) and L4 (rest). 1 entity (413) with false IFRS17CalculationEntity and 1 entity as External.

-- COMMAND ----------

select distinct insurerid,a.EntityId,name,ParentEntityId,IFRS17CalculationEntity from entities  a left join hierarchies b on (a.EntityId=b.EntityId) where insurerid in (select distinct topartyid from cashflows)

-- COMMAND ----------

-- MAGIC %md We notice that the Cashflows "ToParty" transactions at level 3 and 4. An Example:

-- COMMAND ----------

select *  from cashflows where topartyid in ('15319619','15760182') -- ---> 15319619 L3 and 15760182 LV4

-- COMMAND ----------

-- MAGIC %md #The number of rows after joining Cashflows + Contracts

-- COMMAND ----------

select count(*) from contracts a left join cashflows b on (a.contractid=b.contractid)

-- COMMAND ----------

-- MAGIC %md #The number of rows after joining Cashflows + Contracts + DateDistributions2

-- COMMAND ----------

select count(*) from contracts a left join cashflows b on (a.contractid=b.contractid) left join datedistributions2 c on (b.DateDistributionid=c.DateDistributionid)

-- COMMAND ----------

-- MAGIC %md number of records of distinct values in cashflows

-- COMMAND ----------

create view default.DistinctCashflows as
select distinct
CONTRACTID,
CASHFLOWID,
CASHFLOWTYPE,
CASHFLOWSUBTYPE,
FROMPARTYID,
TOPARTYID,
DATEDISTRIBUTIONID,
VALUATIONDATE,
FUTURESTATEID,
FUTURESTATEPROBABILITY,
RISKCOUNTERPARTYID,
COUNTRYOFRISK,
DATASOURCE,
MODELID,
INVOICEID,
CLAIMID,
FLOWSEQUENCEID,
RISKCURRENCY,
TRANSACTIONCURRENCY,
AMOUNT,
COUNTERPARTYDEFAULTADJUSTMENT
from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.CAshflows


-- COMMAND ----------

select count(*) from default.DistinctCashflows

-- COMMAND ----------

select cashflowtype,sum(amount) from default.DistinctCashflows group by cashflowtype

-- COMMAND ----------

-- MAGIC %md Analysis of the distributions of CashflowTypes across RiskPeriodEndDate. You can join sepratley the query below and join them by cashflowtype and contract

-- COMMAND ----------

select ContractId,Count(Distinct CashflowType) from MI2022.cashflows_201912_20220413_133443_206 where Contractid in (select distinct contractid from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206) group by ContractId having Count(Distinct CashflowType)<3 order by 2 asc

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206 where contractid='CYC / 30074941 / 20040601'

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206 where CashFlowId=9022482234002859549

-- COMMAND ----------

select distinct a.ContractId,CashflowType,a.RiskPeriodEndDate from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid order by contractid

-- COMMAND ----------

With P as 
(
select distinct a.ContractId,CashflowType,a.RiskPeriodEndDate from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid where cashflowtype='P'
),
C as
(
select distinct a.ContractId,CashflowType,a.RiskPeriodEndDate from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid where cashflowtype='C'
),
E as,
(
select distinct a.ContractId,CashflowType,a.RiskPeriodEndDate from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid where cashflowtype='E'
)
select * from P p inner join C c on p.ContractId=c.ContractId inner join E e on c.Contractid=e.Contractid where p.RiskPeriodEndDate!=e.RiskPeriodEndDate
