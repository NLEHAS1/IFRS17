-- Databricks notebook source
	
2019Q4:  201912_20220522_180045_173

2020Q1:  202003_20220522_180045_174

2020Q2 : 202006_20220522_180045_175

2020Q3 : 202009_20220522_180045_176

2020Q4 : 202012_20220522_180045_177

-- COMMAND ----------

-- MAGIC %md #201912_20220522_180045_173

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220522_180045_173/Contracts")

-- COMMAND ----------

CREATE table MI2022.cashflows_201912_20220522_180045_173
USING orc OPTIONS (path "/mnt/sl2/201912_20220522_180045_173/CashFlows/1");
CREATE table MI2022.contracts_201912_20220522_180045_173
USING orc OPTIONS (path "/mnt/sl2/201912_20220522_180045_173/Contracts/1");
CREATE table MI2022.datedistributions_201912_20220522_180045_173
USING orc OPTIONS (path "/mnt/sl2/201912_20220522_180045_173/DateDistributions/1");
CREATE table MI2022.entities_201912_20220522_180045_173
USING orc OPTIONS (path "/mnt/sl2/201912_20220522_180045_173/Entities/1");
CREATE table MI2022.fxrates_201912_20220522_180045_173
USING orc OPTIONS (path "/mnt/sl2/201912_20220522_180045_173/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220522_180045_173

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_201912_20220522_180045_173

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220522_180045_173

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_201912_20220522_180045_173

-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220522_180045_173

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_201912_20220522_180045_173

-- COMMAND ----------

select count(*) from MI2022.entities_201912_20220522_180045_173

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_201912_20220522_180045_173

-- COMMAND ----------

select count(*) from MI2022.fxrates_201912_20220522_180045_173

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_201912_20220522_180045_173

-- COMMAND ----------

create table mireporting.FxRates2_201912_20220522_180045_173 (
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

insert into mireporting.FxRates2_201912_20220522_180045_173  values (
20191231,20191231,20191231,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_201912_20220522_180045_173 as 
select * from mireporting.FxRates2_201912_20220522_180045_173
union all 
select * from MI2022.fxrates_201912_20220522_180045_173

-- COMMAND ----------

select * from mi2022.FxRates3_201912_20220522_180045_173

-- COMMAND ----------

-- MAGIC %md Simple DQ check

-- COMMAND ----------

desc MI2022.cashflows_201912_20220522_180045_173

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
count(*)  from MI2022.cashflows_201912_20220522_180045_173
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

create view MI2022.cashflows2_201912_20220522_180045_173 as
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
from MI2022.cashflows_201912_20220522_180045_173

-- COMMAND ----------

select distinct contractid from MI2022.cashflows_201912_20220522_180045_173
except
select distinct contractid from MI2022.contracts_201912_20220522_180045_173

-- COMMAND ----------

select distinct contractid from MI2022.contracts_201912_20220522_180045_173
except
select distinct contractid from MI2022.cashflows_201912_20220522_180045_173

-- COMMAND ----------

select distinct datedistributionid from MI2022.cashflows_201912_20220522_180045_173
except
select distinct datedistributionid from MI2022.datedistributions_201912_20220522_180045_173

-- COMMAND ----------

select distinct datedistributionid from MI2022.datedistributions_201912_20220522_180045_173
except
select distinct datedistributionid from MI2022.cashflows_201912_20220522_180045_173

-- COMMAND ----------

-- MAGIC %md #202003_20220522_180045_174

-- COMMAND ----------

CREATE table MI2022.cashflows_202003_20220522_180045_174
USING orc OPTIONS (path "/mnt/sl2/202003_20220522_180045_174/CashFlows/1");
CREATE table MI2022.contracts_202003_20220522_180045_174
USING orc OPTIONS (path "/mnt/sl2/202003_20220522_180045_174/Contracts/1");
CREATE table MI2022.datedistributions_202003_20220522_180045_174
USING orc OPTIONS (path "/mnt/sl2/202003_20220522_180045_174/DateDistributions/1");
CREATE table MI2022.entities_202003_20220522_180045_174
USING orc OPTIONS (path "/mnt/sl2/202003_20220522_180045_174/Entities/1");
CREATE table MI2022.fxrates_202003_20220522_180045_174
USING orc OPTIONS (path "/mnt/sl2/202003_20220522_180045_174/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202003_20220522_180045_174

-- COMMAND ----------

select distinct valuationDate from MI2022.cashflows_202003_20220522_180045_174

-- COMMAND ----------

select count(*) from MI2022.contracts_202003_20220522_180045_174

-- COMMAND ----------

select distinct valuationDate from MI2022.contracts_202003_20220522_180045_174

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202003_20220522_180045_174

-- COMMAND ----------

select distinct valuationDate from MI2022.datedistributions_202003_20220522_180045_174

-- COMMAND ----------

select count(*) from MI2022.entities_202003_20220522_180045_174

-- COMMAND ----------

select distinct valuationDate from MI2022.entities_202003_20220522_180045_174

-- COMMAND ----------

select count(*) from MI2022.fxrates_202003_20220522_180045_174

-- COMMAND ----------

select distinct valuationDate from MI2022.fxrates_202003_20220522_180045_174

-- COMMAND ----------

create table mireporting.FxRates2_202003_20220522_180045_174 (
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

insert into mireporting.FxRates2_202003_20220522_180045_174  values (
20200331,20200331,20200331,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202003_20220522_180045_174 as 
select * from mireporting.FxRates2_202003_20220522_180045_174
union all 
select * from MI2022.fxrates_202003_20220522_180045_174

-- COMMAND ----------

select * from mi2022.FxRates3_202003_20220522_180045_174

-- COMMAND ----------

create view MI2022.cashflows2_202003_20220522_180045_174 as
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
from MI2022.cashflows_202003_20220522_180045_174

-- COMMAND ----------

select distinct contractid from MI2022.cashflows_202003_20220522_180045_174
except
select distinct contractid from MI2022.contracts_202003_20220522_180045_174

-- COMMAND ----------

select distinct contractid from MI2022.contracts_202003_20220522_180045_174
except
select distinct contractid from MI2022.cashflows_202003_20220522_180045_174

-- COMMAND ----------

select distinct datedistributionid from MI2022.datedistributions_202003_20220522_180045_174
except
select distinct datedistributionid from MI2022.cashflows_202003_20220522_180045_174

-- COMMAND ----------

select distinct datedistributionid from MI2022.cashflows_202003_20220522_180045_174
except
select distinct datedistributionid from MI2022.datedistributions_202003_20220522_180045_174

-- COMMAND ----------

-- MAGIC %md #202006_20220522_180045_175

-- COMMAND ----------

CREATE table MI2022.cashflows_202006_20220522_180045_175
USING orc OPTIONS (path "/mnt/sl2/202006_20220522_180045_175/CashFlows/1");
CREATE table MI2022.contracts_202006_20220522_180045_175
USING orc OPTIONS (path "/mnt/sl2/202006_20220522_180045_175/Contracts/1");
CREATE table MI2022.datedistributions_202006_20220522_180045_175
USING orc OPTIONS (path "/mnt/sl2/202006_20220522_180045_175/DateDistributions/1");
CREATE table MI2022.entities_202006_20220522_180045_175
USING orc OPTIONS (path "/mnt/sl2/202006_20220522_180045_175/Entities/1");
CREATE table MI2022.fxrates_202006_20220522_180045_175
USING orc OPTIONS (path "/mnt/sl2/202006_20220522_180045_175/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202006_20220522_180045_175

-- COMMAND ----------

select distinct valuationDate from MI2022.cashflows_202006_20220522_180045_175

-- COMMAND ----------

select count(*) from MI2022.contracts_202006_20220522_180045_175

-- COMMAND ----------

select distinct valuationDate from MI2022.contracts_202006_20220522_180045_175

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202006_20220522_180045_175

-- COMMAND ----------

select distinct valuationDate from MI2022.datedistributions_202006_20220522_180045_175

-- COMMAND ----------

select count(*) from MI2022.entities_202006_20220522_180045_175

-- COMMAND ----------

-- MAGIC %md Issue

-- COMMAND ----------

select distinct valuationDate from MI2022.entities_202006_20220522_180045_175

-- COMMAND ----------

select count(*) from MI2022.fxrates_202006_20220522_180045_175

-- COMMAND ----------

select distinct valuationDate from MI2022.fxrates_202006_20220522_180045_175

-- COMMAND ----------

create table mireporting.FxRates2_202006_20220522_180045_175 (
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

insert into mireporting.FxRates2_202006_20220522_180045_175  values (
20200630,20200630,20200630,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202006_20220522_180045_175 as 
select * from mireporting.FxRates2_202006_20220522_180045_175
union all 
select * from MI2022.fxrates_202006_20220522_180045_175

-- COMMAND ----------

select * from mi2022.FxRates3_202006_20220522_180045_175

-- COMMAND ----------

create view MI2022.cashflows2_202006_20220522_180045_175 as
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
from MI2022.cashflows_202006_20220522_180045_175

-- COMMAND ----------

select distinct contractid from MI2022.cashflows_202006_20220522_180045_175
except
select distinct contractid from MI2022.contracts_202006_20220522_180045_175

-- COMMAND ----------

select distinct contractid from MI2022.contracts_202006_20220522_180045_175
except
select distinct contractid from MI2022.cashflows_202006_20220522_180045_175

-- COMMAND ----------

select distinct datedistributionid from MI2022.datedistributions_202006_20220522_180045_175
except
select distinct datedistributionid from MI2022.cashflows_202006_20220522_180045_175

-- COMMAND ----------

select distinct datedistributionid from MI2022.cashflows_202006_20220522_180045_175
except
select distinct datedistributionid from MI2022.datedistributions_202006_20220522_180045_175

-- COMMAND ----------

-- MAGIC %md #202009_20220522_180045_176

-- COMMAND ----------

CREATE table MI2022.cashflows_202009_20220522_180045_176
USING orc OPTIONS (path "/mnt/sl2/202009_20220522_180045_176/CashFlows/1");
CREATE table MI2022.contracts_202009_20220522_180045_176
USING orc OPTIONS (path "/mnt/sl2/202009_20220522_180045_176/Contracts/1");
CREATE table MI2022.datedistributions_202009_20220522_180045_176
USING orc OPTIONS (path "/mnt/sl2/202009_20220522_180045_176/DateDistributions/1");
CREATE table MI2022.entities_202009_20220522_180045_176
USING orc OPTIONS (path "/mnt/sl2/202009_20220522_180045_176/Entities/1");
CREATE table MI2022.fxrates_202009_20220522_180045_176
USING orc OPTIONS (path "/mnt/sl2/202009_20220522_180045_176/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202009_20220522_180045_176

-- COMMAND ----------

select distinct valuationDate from MI2022.cashflows_202009_20220522_180045_176

-- COMMAND ----------

select count(*) from MI2022.contracts_202009_20220522_180045_176

-- COMMAND ----------

select distinct valuationDate from MI2022.contracts_202009_20220522_180045_176

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202009_20220522_180045_176

-- COMMAND ----------

select distinct valuationDate from MI2022.datedistributions_202009_20220522_180045_176

-- COMMAND ----------

select count(*) from MI2022.entities_202009_20220522_180045_176

-- COMMAND ----------

-- MAGIC %md Issue

-- COMMAND ----------

select distinct valuationDate from MI2022.entities_202009_20220522_180045_176

-- COMMAND ----------

select count(*) from MI2022.fxrates_202009_20220522_180045_176

-- COMMAND ----------

select distinct valuationDate from MI2022.fxrates_202009_20220522_180045_176

-- COMMAND ----------

create table mireporting.FxRates2_202009_20220522_180045_176 (
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

insert into mireporting.FxRates2_202009_20220522_180045_176  values (
20200930,20200930,20200930,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202009_20220522_180045_176 as 
select * from mireporting.FxRates2_202009_20220522_180045_176
union all 
select * from MI2022.fxrates_202009_20220522_180045_176

-- COMMAND ----------

select * from mi2022.FxRates3_202009_20220522_180045_176

-- COMMAND ----------

create view MI2022.cashflows2_202009_20220522_180045_176 as
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
from MI2022.cashflows_202009_20220522_180045_176

-- COMMAND ----------

select distinct contractid from MI2022.cashflows_202009_20220522_180045_176
except
select distinct contractid from MI2022.contracts_202009_20220522_180045_176

-- COMMAND ----------

select distinct contractid from MI2022.contracts_202009_20220522_180045_176
except
select distinct contractid from MI2022.cashflows_202009_20220522_180045_176

-- COMMAND ----------

select distinct datedistributionid from MI2022.datedistributions_202009_20220522_180045_176
except
select distinct datedistributionid from MI2022.cashflows_202009_20220522_180045_176

-- COMMAND ----------

select distinct datedistributionid from MI2022.cashflows_202009_20220522_180045_176
except
select distinct datedistributionid from MI2022.datedistributions_202009_20220522_180045_176

-- COMMAND ----------

-- MAGIC %md #202012_20220522_180045_177

-- COMMAND ----------

CREATE table MI2022.cashflows_202012_20220522_180045_177
USING orc OPTIONS (path "/mnt/sl2/202012_20220522_180045_177/CashFlows/1");
CREATE table MI2022.contracts_202012_20220522_180045_177
USING orc OPTIONS (path "/mnt/sl2/202012_20220522_180045_177/Contracts/1");
CREATE table MI2022.datedistributions_202012_20220522_180045_177
USING orc OPTIONS (path "/mnt/sl2/202012_20220522_180045_177/DateDistributions/1");
CREATE table MI2022.entities_202012_20220522_180045_177
USING orc OPTIONS (path "/mnt/sl2/202012_20220522_180045_177/Entities/1");
CREATE table MI2022.fxrates_202012_20220522_180045_177
USING orc OPTIONS (path "/mnt/sl2/202012_20220522_180045_177/FxRates/1")

-- COMMAND ----------

select count(*) from MI2022.cashflows_202012_20220522_180045_177

-- COMMAND ----------

select distinct valuationDate from MI2022.cashflows_202012_20220522_180045_177

-- COMMAND ----------

select count(*) from MI2022.contracts_202012_20220522_180045_177

-- COMMAND ----------

select distinct valuationDate from MI2022.contracts_202012_20220522_180045_177

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220522_180045_177

-- COMMAND ----------

select distinct valuationDate from MI2022.datedistributions_202012_20220522_180045_177

-- COMMAND ----------

select count(*) from MI2022.entities_202012_20220522_180045_177

-- COMMAND ----------

-- MAGIC %md Issue

-- COMMAND ----------

select distinct valuationDate from MI2022.entities_202012_20220522_180045_177

-- COMMAND ----------

select count(*) from MI2022.fxrates_202012_20220522_180045_177

-- COMMAND ----------

select distinct valuationDate from MI2022.fxrates_202012_20220522_180045_177

-- COMMAND ----------

create table mireporting.FxRates2_202012_20220522_180045_177 (
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

insert into mireporting.FxRates2_202012_20220522_180045_177  values (
20201231,20201231,20201231,'*','EUR','EUR',1,'None','None','IFRS17'
);

create view mi2022.FxRates3_202012_20220522_180045_177 as 
select * from mireporting.FxRates2_202012_20220522_180045_177
union all 
select * from MI2022.fxrates_202012_20220522_180045_177

-- COMMAND ----------

select * from mi2022.FxRates3_202012_20220522_180045_177

-- COMMAND ----------

create view MI2022.cashflows2_202012_20220522_180045_177 as
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
from MI2022.cashflows_202012_20220522_180045_177

-- COMMAND ----------

select distinct contractid from MI2022.cashflows_202012_20220522_180045_177
except
select distinct contractid from MI2022.contracts_202012_20220522_180045_177

-- COMMAND ----------

select distinct contractid from MI2022.contracts_202012_20220522_180045_177
except
select distinct contractid from MI2022.cashflows_202012_20220522_180045_177

-- COMMAND ----------

select distinct datedistributionid from MI2022.cashflows_202012_20220522_180045_177
except
select distinct datedistributionid from MI2022.datedistributions_202012_20220522_180045_177

-- COMMAND ----------

select distinct datedistributionid from MI2022.datedistributions_202012_20220522_180045_177
except
select distinct datedistributionid from MI2022.cashflows_202012_20220522_180045_177
