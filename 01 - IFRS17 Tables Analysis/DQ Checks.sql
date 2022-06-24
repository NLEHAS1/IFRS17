-- Databricks notebook source
use db_202003_20220621_080921_638; show tables

-- COMMAND ----------

-- MAGIC %md #Contracts Table

-- COMMAND ----------

select count(*) from Contracts

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ContractsPy = sqlContext.sql('Select * from  Contracts')

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC Contractsnull=ContractsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in ContractsPy.columns])
-- MAGIC display(Contractsnull)

-- COMMAND ----------

SELECT contractid, COUNT(contractid)
FROM Contracts
GROUP BY contractid
HAVING COUNT(contractid)>1
order by 2 desc

-- COMMAND ----------

select count(distinct contractid) from Contracts

-- COMMAND ----------

select count(*) from Contracts

-- COMMAND ----------

SELECT contractid, COUNT( distinct ManagedTogetherId)
FROM Contracts
GROUP BY contractid
HAVING COUNT( distinct ManagedTogetherId)>1
order by 2 desc

-- COMMAND ----------

SELECT contractid, COUNT( distinct policyid)
FROM Contracts
GROUP BY contractid
HAVING COUNT( distinct policyid)>1
order by 2 desc

-- COMMAND ----------

select contractid from Contracts
except 
select contractid from Cashflows

-- COMMAND ----------

select contractid from Cashflows
except 
select contractid from Contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

SELECT contractid, COUNT( distinct contractissuedate)
FROM Contracts
GROUP BY contractid
HAVING COUNT( distinct contractissuedate)>1
order by 2 desc

-- COMMAND ----------

SELECT policyid, COUNT( distinct InsuredId)
FROM Contracts
GROUP BY policyid
HAVING COUNT( distinct InsuredId)>1
order by 2 desc

-- COMMAND ----------

select distinct policyid from Contracts where datasource='SYM' and ContractissueDate<> contractinceptiondate

-- COMMAND ----------

select * from  Contracts where mainunit like 'SPU%' and MainProduct not like 'SPU%'

-- COMMAND ----------

select * from  Contracts where mainunit like 'GLB%' and MainProduct not like 'CI%'

-- COMMAND ----------

select * from  Contracts where mainunit like 'LOC%' and MainProduct not like 'CI%'

-- COMMAND ----------

select * from  Contracts where mainunit like 'BON%' and MainProduct not like 'BO%'

-- COMMAND ----------

select * from  Contracts where mainunit like 'INW%' and MainProduct not like 'RE%'

-- COMMAND ----------

-- MAGIC %md #Cashflows

-- COMMAND ----------

select count(*) from Cashflows

-- COMMAND ----------

-- MAGIC %py
-- MAGIC CashflowsPy = sqlContext.sql('Select * from Cashflows')

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC cashnull=CashflowsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in CashflowsPy.columns])
-- MAGIC display(cashnull)

-- COMMAND ----------

select count(*) from cashflows a left join datedistributions b where a.datedistributionid=b.datedistributionid and b.CashflowDate>b.ValuationDate and invoiceid is not null

-- COMMAND ----------

select distinct datasource from cashflows a left join datedistributions b where a.datedistributionid=b.datedistributionid and b.CashflowDate>b.ValuationDate and invoiceid is not null

-- COMMAND ----------

select distinct ValuationDate from Cashflows

-- COMMAND ----------

SELECT cashflowid, COUNT(cashflowid)
FROM Cashflows
where FlowSequenceId=0
GROUP BY cashflowid
HAVING COUNT(cashflowid)>1 
order by 2 asc

-- COMMAND ----------

select distinct cashflowid, count(distinct DateDistributionId)
from Cashflows
group by cashflowid
having count(distinct DateDistributionId)>1
order by 2 desc

-- COMMAND ----------

select distinct cashflowid, count(distinct TransactionCurrency)
from Cashflows
group by cashflowid
having count(distinct TransactionCurrency)>1
order by 2 desc

-- COMMAND ----------

select count(*) from  Cashflows where amount=0 

-- COMMAND ----------

select datasource,count(*) from Cashflows where amount=0 group by datasource 

-- COMMAND ----------

select datasource,count(*) from Cashflows where amount<0 group by datasource 

-- COMMAND ----------

select 
ValuationDate,
cashflowid,
FlowSequenceId ,
ValuationDate,
FutureStateGroupId ,
Count(*)
from Cashflows
group by 
ValuationDate,
cashflowid,
FlowSequenceId ,
ValuationDate,
FutureStateGroupId 
having Count(*)>1
order by 6 desc

-- COMMAND ----------

desc Cashflows

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
from Cashflows
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
from Cashflows where Datasource != 'RE2021'
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

select
  *
from
  Cashflows a
  left join datedistributions b on a.DateDistributionId = b.DateDistributionId
where
  cashflowdate < 19990101
  and transactioncurrency = 'EUR'
order by
  cashflowdate asc

-- COMMAND ----------

-- MAGIC %md #Datedistributions

-- COMMAND ----------

select count(*) from Datedistributions

-- COMMAND ----------

-- MAGIC %py
-- MAGIC DatedistributionsPy = sqlContext.sql('Select * from Datedistributions')

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC DatedistributionsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in DatedistributionsPy.columns]).show()

-- COMMAND ----------

select DateDistributionId from Datedistributions
except
select DateDistributionId from Cashflows

-- COMMAND ----------

select DateDistributionId from Cashflows
except
select DateDistributionId from Datedistributions

-- COMMAND ----------

select distinct ValuationDate from Datedistributions

-- COMMAND ----------

SELECT datedistributionid, COUNT(datedistributionid)
FROM Datedistributions
where weight=1
GROUP BY datedistributionid
HAVING COUNT(datedistributionid)>1
order by 2 desc

-- COMMAND ----------

select count(*) from Datedistributions where weight<0 

-- COMMAND ----------

select count(*) from Datedistributions where weight>1

-- COMMAND ----------

select count(*)from Datedistributions where weight=0

-- COMMAND ----------

desc Datedistributions 

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
  Datedistributions
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

-- MAGIC %md #FxRates

-- COMMAND ----------

select count(*) from FxRates

-- COMMAND ----------

-- MAGIC %py
-- MAGIC FxRatesPy = sqlContext.sql('Select * from FxRates')

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC FxRatesPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in FxRatesPy.columns]).show()

-- COMMAND ----------

select distinct ConversionType from FxRates

-- COMMAND ----------

select * from fxrates where effectfromdate != effecttodate

-- COMMAND ----------

select distinct ValuationDate from FxRates

-- COMMAND ----------

select
  distinct Transactioncurrency
from
  Cashflows a left anti
  join FxRates b on (
    a.TransactionCurrency = b.FromCurrency
    and tocurrency = 'EUR'
  )

-- COMMAND ----------

select distinct cashflowdate from datedistributions where CashflowDate <= ValuationDate 
except
select effectfromdate from fxrates where tocurrency='EUR'

-- COMMAND ----------

select 
ValuationDate,EffectFromDate,CalculationEntity,FromCurrency,ToCurrency,ConversionType, count(*)
from fxrates
group by ValuationDate,EffectFromDate,CalculationEntity,FromCurrency,ToCurrency,ConversionType
having count(*)>1
order by 7 desc

-- COMMAND ----------

-- MAGIC %md #Entities Table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC EntitiesPy = sqlContext.sql('Select * from Entities')

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC EntitiesNull=EntitiesPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in EntitiesPy.columns])
-- MAGIC display(EntitiesNull)

-- COMMAND ----------

select distinct ValuationDate from Entities

-- COMMAND ----------

select EntityId, count(EntityId) from Entities
group by EntityId
having count(EntityId)>1
order by 2

-- COMMAND ----------

select count(*) from Entities
