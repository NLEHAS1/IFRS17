-- Databricks notebook source
drop view MI2022.StartingPointReports_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md The query missing Insurance and Mainsurance contract goroup id and needs to be updated to implement cashflows2

-- COMMAND ----------

create view MI2022.StartingPointReports_201912_20220413_133443_206_001 as
with DateDistributionsTrans as (
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
    Datedistributionsint2
),

CTE1 as (
  select
  a.ValuationDate,
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
  from
    MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows2_201912_20220413_133443_206 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 j on (c.ToPartyId = j.InsurerId)
),

CTE2 as (
select a.*,
CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (TransactionCurrency != 'EUR'and CustomCashflowDate > e.ValuationDate) then e.ConversionRate ---> Future Cashflows
    When (TransactionCurrency != 'EUR'and CustomCashflowDate <= f.ValuationDate) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
   End as ConversionRate,
  CASE
      When TransactionCurrency = 'EUR' then Amount
      When (TransactionCurrency != 'EUR'and CustomCashflowDate > e.ValuationDate) then Amount * e.ConversionRate ---> Future Cashflows
      When (TransactionCurrency != 'EUR'and CustomCashflowDate <= f.ValuationDate) then Amount * f.ConversionRate ---> Actual Cashflows
      Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
    End as AmountEUR
  from
  CTE1 a
    LEFT JOIN MI2022.FxRates3_201912_20220413_133443_206 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_201912_20220413_133443_206 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
),

CTE3 as ( --Only Atradius N.V
  select
    a.*,
    case
      When InsurerId = FromPartyId then AmountEUR * -1
      Else AmountEUR
    End as CustomValue
  from
    CTE2 a
),

CTECT as (
  select
    a.*,
    case
      when b.direct_contract_flg = 1
      and b.rein_contract_flg = 0 then "Direct Contract"
      When b.rein_contract_flg = 1
      and b.direct_contract_flg = 0 then "Reinsurance Held Contract"
      Else "IntraGroup"
    End as ContractType,
    b.INSURANCE_CONTRACT_GROUP_ID,
    b.MAIN_INSURANCE_CONTRACT_GROUP_ID
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.Contractid = b.contractid
      and GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001 
    )
)
select * from CTECT

-- COMMAND ----------

-- MAGIC %md The code below show us that Currncies that have no exchange rate at the cashflowdate will pick the one for valuationdate

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='SYM / 102226 / 19940701' and cashflowid='30357359511473376'

-- COMMAND ----------

-- MAGIC %md #Validations

-- COMMAND ----------

-- MAGIC %md Validate TransactionCurrency !=Eur and Actual

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='SYM / 296436 / 20131201' and cashflowid='30357355216560167'

-- COMMAND ----------

-- MAGIC %md Validate TransactionCurrency !=Eur and Futures

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='2524@-1@11253225@15760144' and cashflowid='296133192077839315'

-- COMMAND ----------

-- MAGIC %md Validate intra Group

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='230@-1@15381551@13235554'

-- COMMAND ----------

-- MAGIC %md Validate Euro Future Cashflows

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='CYC / 30086359 / 20161101' and cashflowid='533542110549267521'

-- COMMAND ----------

-- MAGIC %Validate Euro Actual

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='1425@-1@17299953@14953757' and cashflowid='8916262260813429408'


-- COMMAND ----------

-- MAGIC %md Cashflows has duplicates

-- COMMAND ----------

create view MI2022.cashflows2_201912_20220413_133443_206 as
select distinct 
ValuationDate,
FutureStateGroupId,
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
from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select * from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b where
      GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001 and intra_group_elimination_flg=1

-- COMMAND ----------



-- COMMAND ----------

select * from Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b where
      GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001 and intra_group_elimination_flg is not null

-- COMMAND ----------



-- COMMAND ----------

show tables in mi2022

-- COMMAND ----------

select * from mi2022.groupings_202012_20220413_140845_207

-- COMMAND ----------

select * from mi2022.groupings2_202012_20220413_140845_207

