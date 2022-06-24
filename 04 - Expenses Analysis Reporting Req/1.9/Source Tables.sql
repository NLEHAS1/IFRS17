-- Databricks notebook source
-- MAGIC %md #201912_20220610_000047_498

-- COMMAND ----------

show tables in db_202009_20220610_000047_501

-- COMMAND ----------

create schema 201912_20220610_000047_498

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2/201912_20220610_000047_498/Contracts/")

-- COMMAND ----------

CREATE table 201912_20220610_000047_498.cashflows
USING orc OPTIONS (path "/mnt/sl2/201912_20220610_000047_498/CashFlows/1");
CREATE table 201912_20220610_000047_498.contracts
USING orc OPTIONS (path "/mnt/sl2/201912_20220610_000047_498/Contracts/1");
CREATE table 201912_20220610_000047_498.datedistributions
USING orc OPTIONS (path "/mnt/sl2/201912_20220610_000047_498/DateDistributions/1");
CREATE table 201912_20220610_000047_498.entities
USING orc OPTIONS (path "/mnt/sl2/201912_20220610_000047_498/Entities/1");
CREATE table 201912_20220610_000047_498.fxrates
USING orc OPTIONS (path "/mnt/sl2/201912_20220610_000047_498/FxRates/1")

-- COMMAND ----------

use 201912_20220610_000047_498

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

select distinct ValuationDate from cashflows

-- COMMAND ----------

select count(*) from contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select distinct ValuationDate from datedistributions

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select distinct ValuationDate from entities

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

select distinct ValuationDate from fxrates

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

from cashflows where Datasource != 'RE2021'
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

-- MAGIC %md #202003_20220610_000047_499

-- COMMAND ----------

Create schema 202003_20220610_000047_499

-- COMMAND ----------

CREATE table 202003_20220610_000047_499.cashflows
USING orc OPTIONS (path "/mnt/sl2/202003_20220610_000047_499/CashFlows/1");
CREATE table 202003_20220610_000047_499.contracts
USING orc OPTIONS (path "/mnt/sl2/202003_20220610_000047_499/Contracts/1");
CREATE table 202003_20220610_000047_499.datedistributions
USING orc OPTIONS (path "/mnt/sl2/202003_20220610_000047_499/DateDistributions/1");
CREATE table 202003_20220610_000047_499.entities
USING orc OPTIONS (path "/mnt/sl2/202003_20220610_000047_499/Entities/1");
CREATE table 202003_20220610_000047_499.fxrates
USING orc OPTIONS (path "/mnt/sl2/202003_20220610_000047_499/FxRates/1")

-- COMMAND ----------

use 202003_20220610_000047_499;

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

select distinct ValuationDate from cashflows

-- COMMAND ----------

select count(*) from contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select distinct ValuationDate from datedistributions

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select distinct ValuationDate from entities

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

select distinct ValuationDate from fxrates

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

from cashflows where Datasource != 'RE2021'
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

-- MAGIC %md #202006_20220610_000047_500

-- COMMAND ----------

create schema 202006_20220610_000047_500

-- COMMAND ----------

CREATE table 202006_20220610_000047_500.cashflows
USING orc OPTIONS (path "/mnt/sl2/202006_20220610_000047_500/CashFlows/1");
CREATE table 202006_20220610_000047_500.contracts
USING orc OPTIONS (path "/mnt/sl2/202006_20220610_000047_500/Contracts/1");
CREATE table 202006_20220610_000047_500.datedistributions
USING orc OPTIONS (path "/mnt/sl2/202006_20220610_000047_500/DateDistributions/1");
CREATE table 202006_20220610_000047_500.entities
USING orc OPTIONS (path "/mnt/sl2/202006_20220610_000047_500/Entities/1");
CREATE table 202006_20220610_000047_500.fxrates
USING orc OPTIONS (path "/mnt/sl2/202006_20220610_000047_500/FxRates/1")

-- COMMAND ----------

use 202006_20220610_000047_500

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

select distinct ValuationDate from cashflows

-- COMMAND ----------

select count(*) from contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select distinct ValuationDate from datedistributions

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select distinct ValuationDate from entities

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

select distinct ValuationDate from fxrates

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

from cashflows where Datasource != 'RE2021'
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

-- MAGIC %md #202009_20220610_000047_501

-- COMMAND ----------

create schema 202009_20220610_000047_501

-- COMMAND ----------

CREATE table 202009_20220610_000047_501.cashflows
USING orc OPTIONS (path "/mnt/sl2/202009_20220610_000047_501/CashFlows/1");
CREATE table 202009_20220610_000047_501.contracts
USING orc OPTIONS (path "/mnt/sl2/202009_20220610_000047_501/Contracts/1");
CREATE table 202009_20220610_000047_501.datedistributions
USING orc OPTIONS (path "/mnt/sl2/202009_20220610_000047_501/DateDistributions/1");
CREATE table 202009_20220610_000047_501.entities
USING orc OPTIONS (path "/mnt/sl2/202009_20220610_000047_501/Entities/1");
CREATE table 202009_20220610_000047_501.fxrates
USING orc OPTIONS (path "/mnt/sl2/202009_20220610_000047_501/FxRates/1")

-- COMMAND ----------

use 202009_20220610_000047_501

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

select distinct ValuationDate from cashflows

-- COMMAND ----------

select count(*) from contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select distinct ValuationDate from datedistributions

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select distinct ValuationDate from entities

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

select distinct ValuationDate from fxrates

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

from cashflows where Datasource != 'RE2021'
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

-- MAGIC %md #202012_20220610_000047_502

-- COMMAND ----------

create schema 202012_20220610_000047_502

-- COMMAND ----------

CREATE table 202012_20220610_000047_502.cashflows
USING orc OPTIONS (path "/mnt/sl2/202012_20220610_000047_502/CashFlows/1");
CREATE table 202012_20220610_000047_502.contracts
USING orc OPTIONS (path "/mnt/sl2/202012_20220610_000047_502/Contracts/1");
CREATE table 202012_20220610_000047_502.datedistributions
USING orc OPTIONS (path "/mnt/sl2/202012_20220610_000047_502/DateDistributions/1");
CREATE table 202012_20220610_000047_502.entities
USING orc OPTIONS (path "/mnt/sl2/202012_20220610_000047_502/Entities/1");
CREATE table 202012_20220610_000047_502.fxrates
USING orc OPTIONS (path "/mnt/sl2/202012_20220610_000047_502/FxRates/1")

-- COMMAND ----------

use 202012_20220610_000047_502

-- COMMAND ----------

select count(*) from cashflows

-- COMMAND ----------

select distinct ValuationDate from cashflows

-- COMMAND ----------

select count(*) from contracts

-- COMMAND ----------

select distinct ValuationDate from contracts

-- COMMAND ----------

select count(*) from datedistributions

-- COMMAND ----------

select distinct ValuationDate from datedistributions

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select distinct ValuationDate from entities

-- COMMAND ----------

select count(*) from fxrates

-- COMMAND ----------

select distinct ValuationDate from fxrates

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

from cashflows where Datasource != 'RE2021'
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

-- MAGIC %md #Expenses

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/Expenses/Tagetik") 

-- COMMAND ----------

create table sl1_20201231_v20220529_03.Expenses
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/Expenses/Tagetik");

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Expenses

-- COMMAND ----------

select count(*) from sl1_20201231_v20220529_03.Expenses

-- COMMAND ----------

select distinct valuationdate from sl1_20201231_v20220529_03.Expenses

-- COMMAND ----------

create schema sl1_20200930_v20220529_03

-- COMMAND ----------

create table sl1_20200930_v20220529_03.Expenses
using orc
options (path="/mnt/sl1/DATA/SL1/20200930.v20220529_03/SourceData/Expenses/Tagetik");

-- COMMAND ----------

show tables in sl1_20200930_v20220529_03

-- COMMAND ----------

select count(*) from sl1_20200930_v20220529_03.Expenses

-- COMMAND ----------

select distinct valuationdate from sl1_20200930_v20220529_03.Expenses

-- COMMAND ----------

create schema sl1_20200630_v20220529_03

-- COMMAND ----------

create table sl1_20200630_v20220529_03.Expenses
using orc
options (path="/mnt/sl1/DATA/SL1/20200630.v20220529_03/SourceData/Expenses/Tagetik");

-- COMMAND ----------

select count(*) from sl1_20200630_v20220529_03.Expenses

-- COMMAND ----------

select distinct valuationdate from sl1_20200630_v20220529_03.Expenses

-- COMMAND ----------

create schema sl1_20200331_v20220529_03

-- COMMAND ----------

create table sl1_20200331_v20220529_03.Expenses
using orc
options (path="/mnt/sl1/DATA/SL1/20200331.v20220529_03/SourceData/Expenses/Tagetik");

-- COMMAND ----------

select count(*) from sl1_20200331_v20220529_03.Expenses

-- COMMAND ----------

select distinct valuationdate from sl1_20200331_v20220529_03.Expenses

-- COMMAND ----------

create schema sl1_20191231_v20220529_03

-- COMMAND ----------

create table sl1_20191231_v20220529_03.Expenses
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220529_03/SourceData/Expenses/Tagetik");

-- COMMAND ----------

select count(*) from sl1_20191231_v20220529_03.Expenses

-- COMMAND ----------

select distinct valuationdate from sl1_20191231_v20220529_03.Expenses
