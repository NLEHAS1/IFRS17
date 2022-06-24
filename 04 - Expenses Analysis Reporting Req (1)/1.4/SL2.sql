-- Databricks notebook source
-- MAGIC %fs rm /mnt/sl2/Ehab

-- COMMAND ----------

-- MAGIC %fs mkdirs /mnt/sl2/Ehab

-- COMMAND ----------

-- MAGIC %fs ls /mnt/sl2/Ehab

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2")

-- COMMAND ----------

create database MI location '/mnt/sl2/Ehab'

-- COMMAND ----------

drop database MI

-- COMMAND ----------

describe database MI

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database db_sl1_20210831_v20210906_01

-- COMMAND ----------

-- MAGIC %fs ls /mnt/sl2/201912_20220413_133443_206/CashFlows/

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/CashFlows")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

drop table MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select distinct OptionId from MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

CREATE table MI2022.cashflows_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/CashFlows/_temp")


-- COMMAND ----------

optimize MI2022.cashflows_201912_20220413_133443_206 zorder by (ContractId,CashflowId)

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

desc extended MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/Contracts")

-- COMMAND ----------

drop table MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

CREATE table MI2022.contracts_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Contracts/_temp")


-- COMMAND ----------

select * from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

drop table MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/FxRates")

-- COMMAND ----------

CREATE table MI2022.fxrates_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/FxRates/_temp")


-- COMMAND ----------

select count(*) from MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

drop table MI2022.Entities_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/Entities")

-- COMMAND ----------

CREATE table MI2022.entities_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Entities/_temp")


-- COMMAND ----------

select * from MI2022.entities2_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.entities_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.entities_201912_20220413_133443_206

-- COMMAND ----------

drop table MI2022.hierarchies_201912_20220413_133443_206

-- COMMAND ----------

CREATE table MI2022.hierarchies_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/Hierarchies/_temp")


-- COMMAND ----------

select * from MI2022.hierarchies_201912_20220413_133443_206

-- COMMAND ----------

drop table MI2022.Datedistributions_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220413_133443_206/DateDistributions")

-- COMMAND ----------

CREATE table MI2022.datedistributions_201912_20220413_133443_206
USING parquet OPTIONS (path "/mnt/sl2/201912_20220413_133443_206/DateDistributions/_temp")


-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.datedistributions_201912_20220413_133443_206

-- COMMAND ----------

select distinct datasource from MI2022.cashflows_201912_20220413_133443_206 

-- COMMAND ----------

select distinct modelid from MI2022.cashflows_201912_20220413_133443_206 

-- COMMAND ----------

-- MAGIC %md #202003_20220412_081208_191

-- COMMAND ----------

CREATE table MI2022.cashflows_202003_20220412_081208_191
USING orc OPTIONS (path "/mnt/sl2/202003_20220412_081208_191/CashFlows/1");
CREATE table MI2022.contracts_202003_20220412_081208_191
USING orc OPTIONS (path "/mnt/sl2/202003_20220412_081208_191/Contracts/1");
CREATE table MI2022.datedistributions_202003_20220412_081208_191
USING orc OPTIONS (path "/mnt/sl2/202003_20220412_081208_191/DateDistributions/1");
CREATE table MI2022.entities_202003_20220412_081208_191
USING orc OPTIONS (path "/mnt/sl2/202003_20220412_081208_191/Entities/1");
CREATE table MI2022.fxrates_202003_20220412_081208_191
USING orc OPTIONS (path "/mnt/sl2/202003_20220412_081208_191/FxRates/1")

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202003_20220412_081208_191

-- COMMAND ----------

select count(*) from MI2022.cashflows_202003_20220412_081208_191

-- COMMAND ----------

select count(*) from MI2022.contracts_202003_20220412_081208_191

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202003_20220412_081208_191

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202003_20220412_081208_191

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202003_20220412_081208_191

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202003_20220412_081208_191

-- COMMAND ----------

select count(*) from MI2022.fxrates_202003_20220412_081208_191

-- COMMAND ----------

select count(*) from MI2022.entities_202003_20220412_081208_191

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202003_20220412_081208_191

-- COMMAND ----------

-- MAGIC %md #202006_20220412_143707_198

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC dubtils.fs.ls("/mnt/sl2/202006_20220412_143707_198/CashFlows/1")

-- COMMAND ----------

CREATE table MI2022.cashflows_202006_20220412_143707_198
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/CashFlows/1");
CREATE table MI2022.contracts_202006_20220412_143707_198
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/Contracts/1");
CREATE table MI2022.datedistributions_202006_20220412_143707_198
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/DateDistributions/1");
CREATE table MI2022.entities_202006_20220412_143707_198
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/Entities/1");
CREATE table MI2022.fxrates_202006_20220412_143707_198
USING orc OPTIONS (path "/mnt/sl2/202006_20220412_143707_198/FxRates/1")

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202006_20220412_143707_198

-- COMMAND ----------

select count(*) from MI2022.contracts_202006_20220412_143707_198

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202006_20220412_143707_198

-- COMMAND ----------

select count(*) from MI2022.cashflows_202006_20220412_143707_198

-- COMMAND ----------

select count(*) from MI2022.cashflows2_202006_20220412_143707_198

-- COMMAND ----------

select count(*)  from MI2022.datedistributions_202006_20220412_143707_198

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202006_20220412_143707_198

-- COMMAND ----------

select count(*) from MI2022.fxrates_202006_20220412_143707_198

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202006_20220412_143707_198

-- COMMAND ----------

select count(*) from MI2022.entities_202006_20220412_143707_198

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202006_20220412_143707_198

-- COMMAND ----------

desc MI2022.cashflows_202006_20220412_143707_198

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
count(*)  from MI2022.cashflows_202006_20220412_143707_198
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

create view MI2022.cashflows2_202006_20220412_143707_198 as
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
from MI2022.cashflows_202006_20220412_143707_198

-- COMMAND ----------

create table mireporting.FxRates22_202006_20220412_143707_198 (
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string

)

-- COMMAND ----------

insert into mireporting.FxRates22_202006_20220412_143707_198  values (
20200630,20200630,20200630,'*','EUR','EUR',1,'None','None','IFRS17'
)

-- COMMAND ----------

drop view mi2022.FxRates3_202006_20220412_143707_198

-- COMMAND ----------

create view mi2022.FxRates3_202006_20220412_143707_198 as 
select * from mireporting.FxRates22_202006_20220412_143707_198 
union all 
select * from MI2022.fxrates_202006_20220412_143707_198

-- COMMAND ----------

select * from mi2022.FxRates3_202006_20220412_143707_198

-- COMMAND ----------

SELECT * from MI2022.datedistributions_202006_20220412_143707_198 where weight is null

-- COMMAND ----------

-- MAGIC %md #202009_20220413_195153_208

-- COMMAND ----------

CREATE table MI2022.cashflows_202009_20220413_195153_208
USING orc OPTIONS (path "/mnt/sl2/202009_20220413_195153_208/CashFlows/1");
CREATE table MI2022.contracts_202009_20220413_195153_208
USING orc OPTIONS (path "/mnt/sl2/202009_20220413_195153_208/Contracts/1");
CREATE table MI2022.datedistributions_202009_20220413_195153_208
USING orc OPTIONS (path "/mnt/sl2/202009_20220413_195153_208/DateDistributions/1");
CREATE table MI2022.entities_202009_20220413_195153_208
USING orc OPTIONS (path "/mnt/sl2/202009_20220413_195153_208/Entities/1");
CREATE table MI2022.fxrates_202009_20220413_195153_208
USING orc OPTIONS (path "/mnt/sl2/202009_20220413_195153_208/FxRates/1")

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_202009_20220413_195153_208

-- COMMAND ----------

select count(*) from MI2022.contracts_202009_20220413_195153_208

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_202009_20220413_195153_208

-- COMMAND ----------

select count(*) from MI2022.cashflows_202009_20220413_195153_208

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202009_20220413_195153_208

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202009_20220413_195153_208

-- COMMAND ----------

select count(*) from MI2022.fxrates_202009_20220413_195153_208

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202009_20220413_195153_208

-- COMMAND ----------

select count(*) from MI2022.entities_202009_20220413_195153_208

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202009_20220413_195153_208

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
count(*)  from MI2022.cashflows_202009_20220413_195153_208
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

create view MI2022.cashflows2_202009_20220413_195153_208 as
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
from MI2022.cashflows_202009_20220413_195153_208

-- COMMAND ----------

create table mireporting.fxrates2_202009_20220413_195153_208(
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string

)

-- COMMAND ----------

insert into mireporting.fxrates2_202009_20220413_195153_208  values (
20200930,20200930,20200930,'*','EUR','EUR',1,'None','None','IFRS17'
)

-- COMMAND ----------

create view MI2022.fxrates3_202009_20220413_195153_208 as 
select * from mireporting.fxrates2_202009_20220413_195153_208
union all 
select * from MI2022.fxrates_202009_20220413_195153_208

-- COMMAND ----------

select * from MI2022.fxrates3_202009_20220413_195153_208

-- COMMAND ----------

-- MAGIC %md #202012_20220413_140845_207

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/202012_20220413_140845_207/CashFlows")

-- COMMAND ----------

CREATE table MI2022.datedistributions_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/DateDistributions/1");
CREATE table MI2022.entities_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/Entities/1");
CREATE table MI2022.fxrates_202012_20220413_140845_207
USING orc OPTIONS (path "/mnt/sl2/202012_20220413_140845_207/FxRates/1")

-- COMMAND ----------

select distinct valuationDate from mi2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from mi2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

select distinct valuationDate from mi2022.contracts_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from mi2022.contracts_202012_20220413_140845_207

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220413_140845_207

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.fxrates_202012_20220413_140845_207

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_202012_20220413_140845_207

-- COMMAND ----------

select count(*) from MI2022.entities_202012_20220413_140845_207

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
count(*)  from MI2022.cashflows_202012_20220413_140845_207
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

create view MI2022.cashflows2_202012_20220413_140845_207 as
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
from MI2022.cashflows_202012_20220413_140845_207

-- COMMAND ----------

create table mireporting.fxrates2_202012_20220413_140845_207 ( 
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

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/data_prdifrs17mi/fxrates2_202012_20220413_140845_207")

-- COMMAND ----------

select * from mireporting.fxrates2_202012_20220413_140845_207

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.rm("/mnt/data_prdifrs17mi/fxrates2_202012_20220413_140845_207",True)

-- COMMAND ----------

VACUUM  mireporting.fxrates2_202012_20220413_140845_207 

-- COMMAND ----------

delete from mireporting.fxrates2_202012_20220413_140845_207

-- COMMAND ----------

create table mireporting.fxrates22_202012_20220413_140845_207 ( 
ValuationDate	int,
EffectFromDate	int,
EffectToDate	int,
CalculationEntity	string,
FromCurrency	string,
ToCurrency	string,
ConversionRate	double,
ConversionType	string,
DataSource	string,
Context	string

);
insert into mireporting.fxrates22_202012_20220413_140845_207  values (
20201231,20201231,20201231,'*','EUR','EUR',1,'None','None','IFRS17'
);
create view MI2022.fxrates3_202012_20220413_140845_207 as 
select * from mireporting.fxrates22_202012_20220413_140845_207
union all 
select * from MI2022.fxrates_202012_20220413_140845_207

-- COMMAND ----------

select * from MI2022.fxrates3_202012_20220413_140845_207

-- COMMAND ----------

show tables in mi2022

-- COMMAND ----------

select * from mi2022.contracts_sl1_20191231_v20220325_01

-- COMMAND ----------

select count(*) from MI2022.datedistributions_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.datedistributions_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.contracts_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.entities_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.entities_201912_20220413_133443_206

-- COMMAND ----------



-- COMMAND ----------

select * from MI2022.cashflows_202012_20220528_010617_253 where datedistributionid=-154197563
