-- Databricks notebook source
use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

drop view MI2022.StartingPointReports_201912_20220413_133443_206_Initial

-- COMMAND ----------

Create view MI2022.StartingPointReports_201912_20220413_133443_206_Initial as 
-- The CTE aggregating Datedistributions table
With DateDistributionsTrans as (
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
      MI2022.datedistributions_201912_20220413_133443_206
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
    Datedistributionsint2)

select
  distinct a.ValuationDate,
  a.ContractId,
  a.DataSource as ContractsDataSource,
  a.PolicyId,
  a.ManagedTogetherId,
  a.InsurerId,
  a.InsuredId,
  a.BeneficiaryId,
  a.CustomerCountry,
  a.CoverStartDate,
  a.CoverEndDate,
  a.BoundDate,
  a.ContractInceptionDate,
  a.ContractIssueDate,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(a.ContractIssueDate, 6) as UnderwritingMonth,
  a.RiskPeriodStartDate,
  a.RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  c.FutureStateGroupId,
  c.FutureStateGroupProbability,
  c.CashFlowId,
  c.CashFlowType,
  c.CashFlowSubType,
  c.FromPartyId,
  c.ToPartyId,
  c.RiskCounterPartyId,
  c.CountryOfRisk,
  c.DataSource as CashflowsDataSource,
  c.ModelId,
  c.InvoiceId,
  c.ClaimId,
  c.FlowSequenceId,
  c.DateDistributionId,
  c.RiskCurrency,
  c.TransactionCurrency,
  c.Amount * d.Weight as Amount,
  d.CustomCashflowDate,
  CASE
    When c.TransactionCurrency = 'EUR' then e.ConversionRate
    When d.CustomCashflowDate > d.ValuationDate then e.ConversionRate ---> Future Cashflows
    When d.CustomCashflowDate <= d.ValuationDate then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When c.TransactionCurrency = 'EUR' then c.Amount * d.weight
    When (c.TransactionCurrency != 'EUR' and d.CustomCashflowDate > d.ValuationDate) then c.Amount * e.ConversionRate * d.weight ---> Future Cashflows
    When ( c.TransactionCurrency != 'EUR'and d.CustomCashflowDate <= d.ValuationDate) then c.Amount * f.ConversionRate * d.weight ---> Actual Cashflows
    Else c.Amount * e.ConversionRate * d.weight -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR,
  CASE
    when d.CustomCashflowDate > d.ValuationDate then "Future Cashflow"
    Else "Actual Cashflow"
  End as CashflowNature,
  g.EntityId as InsurerEntityId,
  g.Name as InsurerName,
  h.EntityId as InsuredEntityId,
  h.Name as InsuredName,
  i.EntityId as FrompartyEntityId,
  i.Name as FrompartyName,
  j.EntityId as TopartyEntityId,
  j.Name as TopartyName
FROM
  MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows_201912_20220413_133443_206 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  -- The only transformation I did for FxRates is to add Euro to Euro exchange rate
  -- The first join of FxRates3 is to exctract the right conversion rate for Future Cashflows
  LEFT JOIN MI2022.FxRates3_201912_20220413_133443_206 e on (c.TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
  -- The second join of FxRates3 is to exctract the right conversion rate for Actual Cashflows
  LEFT JOIN MI2022.FxRates3_201912_20220413_133443_206 f on (c.TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and d.CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 j on (c.ToPartyId = j.InsurerId)


-- COMMAND ----------

-- MAGIC %md I'm creating another view on top of the previous view because the Amount in Euro is needed in subsequent calculations

-- COMMAND ----------

drop view MI2022.StartingPointReports_201912_20220413_133443_206

-- COMMAND ----------

Create view MI2022.StartingPointReports_201912_20220413_133443_206 as
select a.*,
case
When InsurerId=FromPartyId then AmountEUR*-1
Else AmountEUR
End as CustomValue
from MI2022.StartingPointReports_201912_20220413_133443_206_Initial a

-- COMMAND ----------

select count(*) from MI2022.StartingPointReports_201912_20220413_133443_206

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.fxrates2 where FromCurrency='EUR' and ToCurrency='EUR'
union all 
select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.fxrates

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.fxrates2

-- COMMAND ----------

desc db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.fxrates2

-- COMMAND ----------

create table MI2022.FxRates2_201912_20220413_133443_206
(
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

select * from MI2022.FxRates2_201912_20220413_133443_206

-- COMMAND ----------

insert into MI2022.FxRates2_201912_20220413_133443_206
(ValuationDate,
EffectFromDate,
EffectToDate,
CalculationEntity,
FromCurrency,
ToCurrency,
ConversionRate,
ConversionType,
DataSource,
Context
)
VALUES  (20191231,20191231,20191231,'*','EUR','EUR',1,'None','None','IFRS17');

-- COMMAND ----------

create view MI2022.FxRates3_201912_20220413_133443_206 as
select * from MI2022.FxRates2_201912_20220413_133443_206 
union all 
select * from MI2022.FxRates_201912_20220413_133443_206

-- COMMAND ----------

select * from MI2022.FxRates3_201912_20220413_133443_206 where 

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220413_133443_206 where Left(ContractIssueDate, 6)>201512 and datasource='SYM'

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220413_133443_206 where   datasource='SYM'
