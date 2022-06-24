-- Databricks notebook source
-- MAGIC %md #201912_20220603_000035_370

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220603_000035_370/Contracts")

-- COMMAND ----------

CREATE table MI2022.cashflows_201912_20220603_000035_370
USING orc OPTIONS (path "/mnt/sl2/201912_20220603_000035_370/CashFlows/1");
CREATE table MI2022.contracts_201912_20220603_000035_370
USING orc OPTIONS (path "/mnt/sl2/201912_20220603_000035_370/Contracts/1");
CREATE table MI2022.datedistributions_201912_20220603_000035_370
USING orc OPTIONS (path "/mnt/sl2/201912_20220603_000035_370/DateDistributions/1");
CREATE table MI2022.entities_201912_20220603_000035_370
USING orc OPTIONS (path "/mnt/sl2/201912_20220603_000035_370/Entities/1");
CREATE table MI2022.fxrates_201912_20220603_000035_370
USING orc OPTIONS (path "/mnt/sl2/201912_20220603_000035_370/FxRates/1")

-- COMMAND ----------

select count(*) from db_201912_20220603_000035_370.cashflows

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220603_000035_370

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_201912_20220603_000035_370

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220603_000035_370

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_201912_20220603_000035_370

-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220603_000035_370

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_201912_20220603_000035_370

-- COMMAND ----------

select count(*) from MI2022.entities_201912_20220603_000035_370

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_201912_20220603_000035_370

-- COMMAND ----------

select count(*) from MI2022.fxrates_201912_20220603_000035_370

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_201912_20220603_000035_370

-- COMMAND ----------

select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context from MI2022.fxrates_201912_20220603_000035_370
union all 
select * from MI2022.fxrates_201912_20220603_000035_370

-- COMMAND ----------

desc MI2022.cashflows_201912_20220603_000035_370

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

from MI2022.cashflows_201912_20220603_000035_370 where Datasource != 'RE2021'
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

select * from MI2022.contracts_201912_20220603_000035_370

-- COMMAND ----------

select *  from
  MI2022.cashflows_202012_20220528_010617_253 where transactioncurrency='VEB'

-- COMMAND ----------

select * from MI2022.fxrates_202012_20220528_010617_253 where fromcurrency='VEB' and tocurrency='EUR'

-- COMMAND ----------

select *  from
  MI2022.cashflows_202012_20220528_010617_253 where transactioncurrency='PEI'

-- COMMAND ----------

select * from MI2022.fxrates_202012_20220528_010617_253 where fromcurrency='PEI' and tocurrency='EUR'

-- COMMAND ----------

select *  from
 MI2022.cashflows_202006_20220412_143707_198 where cashflowid like '474371136658027%'

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
from MI2022.cashflows_201912_20220603_000035_370 
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

select * from MI2022.cashflows_201912_20220603_000035_370  where cashflowid='4340741625757957315'


-- COMMAND ----------

CREATE table MI2022.cashflows_202003_20220603_000035_371
USING orc OPTIONS (path "/mnt/sl2/202003_20220603_000035_371/CashFlows/1");
CREATE table MI2022.contracts_202003_20220603_000035_371
USING orc OPTIONS (path "/mnt/sl2/202003_20220603_000035_371/Contracts/1");
CREATE table MI2022.datedistributions_202003_20220603_000035_371
USING orc OPTIONS (path "/mnt/sl2/202003_20220603_000035_371/DateDistributions/1");
CREATE table MI2022.entities_202003_20220603_000035_371
USING orc OPTIONS (path "/mnt/sl2/202003_20220603_000035_371/Entities/1");
CREATE table MI2022.fxrates_202003_20220603_000035_371
USING orc OPTIONS (path "/mnt/sl2/202003_20220603_000035_371/FxRates/1")

-- COMMAND ----------


