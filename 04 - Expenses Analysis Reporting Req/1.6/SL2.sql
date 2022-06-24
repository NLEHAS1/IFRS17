-- Databricks notebook source
-- MAGIC %md #201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220528_010617_249/Contracts/1")

-- COMMAND ----------

CREATE table MI2022.cashflows_201912_20220528_010617_249
USING orc OPTIONS (path "/mnt/sl2/201912_20220528_010617_249/CashFlows/1");
CREATE table MI2022.contracts_201912_20220528_010617_249
USING orc OPTIONS (path "/mnt/sl2/201912_20220528_010617_249/Contracts/1");
CREATE table MI2022.datedistributions_201912_20220528_010617_249
USING orc OPTIONS (path "/mnt/sl2/201912_20220528_010617_249/DateDistributions/1");
CREATE table MI2022.entities_201912_20220528_010617_249
USING orc OPTIONS (path "/mnt/sl2/201912_20220528_010617_249/Entities/1");
CREATE table MI2022.fxrates_201912_20220528_010617_249
USING orc OPTIONS (path "/mnt/sl2/201912_20220528_010617_249/FxRates/1")

-- COMMAND ----------

-- MAGIC %py data_df = spark.read.format("orc").load("/mnt/sl2/201912_20220528_010617_249/CashFlows/1")
-- MAGIC display(data_df)

-- COMMAND ----------

-- MAGIC %py data_df.count() 

-- COMMAND ----------

Select * from delta.`dbfs:/mnt/sl2/201912_20220528_010617_249/CashFlows/1`

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220528_010617_249

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_201912_20220528_010617_249

-- COMMAND ----------

select count(*) from MI2022.fxrates_201912_20220528_010617_249

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_201912_20220528_010617_249

-- COMMAND ----------

select count(*) from MI2022.entities_201912_20220528_010617_249

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_201912_20220528_010617_249

-- COMMAND ----------

create table mireporting.FxRates2_201912_20220528_010617_249 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string);

insert into mireporting.FxRates2_201912_20220528_010617_249  values (
20191231,20191231,20191231,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_201912_20220528_010617_249 as 
select * from mireporting.FxRates2_201912_20220528_010617_249
union all 
select * from MI2022.fxrates_201912_20220528_010617_249

-- COMMAND ----------

select * from mi2022.FxRates3_201912_20220528_010617_249

-- COMMAND ----------

create view MI2022.cashflows2_201912_20220528_010617_249 as
select 
distinct
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
from MI2022.cashflows_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md #202003_20220528_010617_250

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202003_20220528_010617_250/Contracts")

-- COMMAND ----------

CREATE table MI2022.cashflows_202003_20220528_010617_250
USING orc OPTIONS (path "/mnt/sl2/202003_20220528_010617_250/CashFlows/1");
CREATE table MI2022.contracts_202003_20220528_010617_250
USING orc OPTIONS (path "/mnt/sl2/202003_20220528_010617_250/Contracts/1");
CREATE table MI2022.datedistributions_202003_20220528_010617_250
USING orc OPTIONS (path "/mnt/sl2/202003_20220528_010617_250/DateDistributions/1");
CREATE table MI2022.entities_202003_20220528_010617_250
USING orc OPTIONS (path "/mnt/sl2/202003_20220528_010617_250/Entities/1");
CREATE table MI2022.fxrates_202003_20220528_010617_250
USING orc OPTIONS (path "/mnt/sl2/202003_20220528_010617_250/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202003_20220528_010617_250

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202003_20220528_010617_250

-- COMMAND ----------

select count(*) from MI2022.contracts_202003_20220528_010617_250

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202003_20220528_010617_250

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202003_20220528_010617_250

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202003_20220528_010617_250

-- COMMAND ----------

select count(*) from MI2022.entities_202003_20220528_010617_250

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202003_20220528_010617_250

-- COMMAND ----------

select count(*) from MI2022.fxrates_202003_20220528_010617_250

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202003_20220528_010617_250

-- COMMAND ----------

create table mireporting.FxRates2_202003_20220528_010617_250 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string);

insert into mireporting.FxRates2_202003_20220528_010617_250  values (
20200331,20200331,20200331,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202003_20220528_010617_250 as 
select * from mireporting.FxRates2_202003_20220528_010617_250
union all 
select * from MI2022.fxrates_202003_20220528_010617_250

-- COMMAND ----------

select * from mi2022.FxRates3_202003_20220528_010617_250

-- COMMAND ----------

desc MI2022.cashflows_202003_20220528_010617_250

-- COMMAND ----------

select distinct datasource from MI2022.cashflows_202003_20220528_010617_250

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
from MI2022.cashflows_202003_20220528_010617_250 where Datasource != 'RE2021'
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

-- COMMAND ----------

select distinct a.datasource,b.datasource from MI2022.contracts_202003_20220528_010617_250 a left join MI2022.cashflows_202003_20220528_010617_250 b on a.contractid=b.contractid

-- COMMAND ----------

create view MI2022.cashflows2_202003_20220528_010617_250 as
select 
distinct
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
from MI2022.cashflows_202003_20220528_010617_250

-- COMMAND ----------

-- MAGIC %md #202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202012_20220528_010617_253/Contracts")

-- COMMAND ----------

CREATE table MI2022.cashflows_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/CashFlows/1");
CREATE table MI2022.contracts_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/Contracts/1");
CREATE table MI2022.datedistributions_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/DateDistributions/1");
CREATE table MI2022.entities_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/Entities/1");
CREATE table MI2022.fxrates_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.contracts_202012_20220528_010617_253

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202012_20220528_010617_253

-- COMMAND ----------

select count(*) from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220528_010617_253

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202012_20220528_010617_253

-- COMMAND ----------

select count(*) from MI2022.fxrates_202012_20220528_010617_253

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202012_20220528_010617_253

-- COMMAND ----------

select count(*) from MI2022.entities_202012_20220528_010617_253

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202012_20220528_010617_253

-- COMMAND ----------

create table mireporting.FxRates2_202012_20220528_010617_253 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string);

insert into mireporting.FxRates2_202012_20220528_010617_253  values (
20201231,20201231,20201231,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202012_20220528_010617_253 as 
select * from mireporting.FxRates2_202012_20220528_010617_253
union all 
select * from MI2022.fxrates_202012_20220528_010617_253

-- COMMAND ----------

select * from mi2022.FxRates3_202012_20220528_010617_253

-- COMMAND ----------

create view MI2022.cashflows2_202012_20220528_010617_253 as
select 
distinct
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
from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md #202006_20220528_010617_251

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202006_20220528_010617_251/Contracts")

-- COMMAND ----------

CREATE table MI2022.cashflows_202006_20220528_010617_251
USING orc OPTIONS (path "/mnt/sl2/202006_20220528_010617_251/CashFlows/1");
CREATE table MI2022.contracts_202006_20220528_010617_251
USING orc OPTIONS (path "/mnt/sl2/202006_20220528_010617_251/Contracts/1");
CREATE table MI2022.datedistributions_202006_20220528_010617_251
USING orc OPTIONS (path "/mnt/sl2/202006_20220528_010617_251/DateDistributions/1");
CREATE table MI2022.entities_202006_20220528_010617_251
USING orc OPTIONS (path "/mnt/sl2/202006_20220528_010617_251/Entities/1");
CREATE table MI2022.fxrates_202006_20220528_010617_251
USING orc OPTIONS (path "/mnt/sl2/202006_20220528_010617_251/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202006_20220528_010617_251

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202006_20220528_010617_251

-- COMMAND ----------

select count(*) from MI2022.contracts_202006_20220528_010617_251

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202006_20220528_010617_251

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202006_20220528_010617_251

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202006_20220528_010617_251

-- COMMAND ----------

select count(*) from MI2022.entities_202006_20220528_010617_251

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202006_20220528_010617_251

-- COMMAND ----------

select count(*) from MI2022.fxrates_202006_20220528_010617_251

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202006_20220528_010617_251

-- COMMAND ----------

create table mireporting.FxRates2_202006_20220528_010617_251 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string);

insert into mireporting.FxRates2_202006_20220528_010617_251  values (
20200630,20200630,20200630,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202006_20220528_010617_251 as 
select * from mireporting.FxRates2_202006_20220528_010617_251
union all 
select * from MI2022.fxrates_202006_20220528_010617_251

-- COMMAND ----------

select * from mi2022.FxRates3_202006_20220528_010617_251

-- COMMAND ----------

-- MAGIC %md #202009_20220528_010617_252

-- COMMAND ----------

CREATE table MI2022.cashflows_202009_20220528_010617_252
USING orc OPTIONS (path "/mnt/sl2/202009_20220528_010617_252/CashFlows/1");
CREATE table MI2022.contracts_202009_20220528_010617_252
USING orc OPTIONS (path "/mnt/sl2/202009_20220528_010617_252/Contracts/1");
CREATE table MI2022.datedistributions_202009_20220528_010617_252
USING orc OPTIONS (path "/mnt/sl2/202009_20220528_010617_252/DateDistributions/1");
CREATE table MI2022.entities_202009_20220528_010617_252
USING orc OPTIONS (path "/mnt/sl2/202009_20220528_010617_252/Entities/1");
CREATE table MI2022.fxrates_202009_20220528_010617_252
USING orc OPTIONS (path "/mnt/sl2/202009_20220528_010617_252/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202009_20220528_010617_252

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202009_20220528_010617_252

-- COMMAND ----------

select count(*) from MI2022.contracts_202009_20220528_010617_252

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202009_20220528_010617_252

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202009_20220528_010617_252

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202009_20220528_010617_252

-- COMMAND ----------

select count(*) from MI2022.entities_202009_20220528_010617_252

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202009_20220528_010617_252

-- COMMAND ----------

select count(*) from MI2022.fxrates_202009_20220528_010617_252

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202009_20220528_010617_252

-- COMMAND ----------

create table mireporting.FxRates2_202009_20220528_010617_252 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string);

insert into mireporting.FxRates2_202009_20220528_010617_252  values (
20200930,20200930,20200930,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202009_20220528_010617_252 as 
select * from mireporting.FxRates2_202009_20220528_010617_252
union all 
select * from MI2022.fxrates_202009_20220528_010617_252

-- COMMAND ----------

select * from mi2022.FxRates3_202009_20220528_010617_252

-- COMMAND ----------

select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context from MI2022.fxrates_202009_20220528_010617_252

-- COMMAND ----------

select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context from MI2022.fxrates_202009_20220528_010617_252
union all 
select * from MI2022.fxrates_202009_20220528_010617_252

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202012_20220528_010617_253/CashFlowMappings")

-- COMMAND ----------

CREATE table MI2022.CashFlowMappings_202012_20220528_010617_253
USING orc OPTIONS (path "/mnt/sl2/202012_20220528_010617_253/CashFlowMappings/1");

-- COMMAND ----------

select * from MI2022.CashFlowMappings_202012_20220528_010617_253

-- COMMAND ----------

select distinct CashFlowType,CashFlowSubType from MI2022.CashFlows_202012_20220528_010617_253

-- COMMAND ----------

select distinct CashFlowSubType from MI2022.CashFlows_202012_20220528_010617_253
except 
select distinct CashFlowSubType from MI2022.CashFlowMappings_202012_20220528_010617_253

-- COMMAND ----------

select distinct CashFlowSubType from MI2022.CashFlowMappings_202012_20220528_010617_253
except 
select distinct CashFlowSubType from MI2022.CashFlows_202012_20220528_010617_253
