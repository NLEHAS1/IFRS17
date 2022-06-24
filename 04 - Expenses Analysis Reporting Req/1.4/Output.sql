-- Databricks notebook source
-- MAGIC %md #Without SAS input

-- COMMAND ----------

Create view mi2022.expenseanalysisagg2 as 
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
  a.InsurerId,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(a.ContractIssueDate, 6) as UnderwritingMonth,
  c.CashFlowId,
  c.CashFlowType,
  c.CashFlowSubType,
  c.FromPartyId,
  c.ToPartyId,
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
  where Left(a.ContractIssueDate, 4) > 2000 and c.Datasource!='RE2021'
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

CTEP as (
  select
    CohortYear,
    CashflowNature,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    CohortYear,
    CashflowNature
),

CTEE as (
  select
    CohortYear,
    CashflowNature,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    CohortYear,
    CashflowNature
),

CTEC as (
  select
    CohortYear,
    CashflowNature,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    CohortYear,
    CashflowNature
)

select
  p.CohortYear,
  p.CashflowNature,
  Premium,
  Expenses,
  Claims,
  Expenses / Premium as ExpenseRatio,
  Claims / Premium as ClaimsRatio,
  (Expenses / Premium) +(Claims / Premium) as CombinedRatio,
  1 +(Expenses / Premium) +(Claims / Premium) as Margin
from
  CTEP p
  inner join CTEE e on (
    p.CohortYear = e.CohortYear
    and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortYear = c.CohortYear
    and  p.CashflowNature=c.CashflowNature
  )

-- COMMAND ----------

-- MAGIC %md # With SAS input

-- COMMAND ----------

drop view mi2022.testexpense

-- COMMAND ----------

Create view mi2022.testexpense as 
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
  a.InsurerId,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(a.ContractIssueDate, 6) as UnderwritingMonth,
  c.CashFlowId,
  c.CashFlowType,
  c.CashFlowSubType,
  c.FromPartyId,
  c.ToPartyId,
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
  where Left(a.ContractIssueDate, 4) > 2000
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
    End as ContractType
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.Contractid = b.contractid
      and GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001 and intra_group_elimination_flg is null)
),

CTEP as (
  select
    CohortYear,
    CashflowNature,
    ContractType,
    Sum(CustomValue) as Premium
  from
    CTECT
  where
    Cashflowtype = 'P'
  group by
    CohortYear,
    ContractType,
    CashflowNature
),

CTEE as (
  select
    CohortYear,
    CashflowNature,
    ContractType,
    Sum(CustomValue) as Expenses
  from
    CTECT
  where
    Cashflowtype = 'E'
  group by
    CohortYear,
    ContractType,
    CashflowNature
),

CTEC as (
  select
    CohortYear,
    ContractType,
    CashflowNature,
    Sum(CustomValue) as Claims
  from
    CTECT
  where
    Cashflowtype = 'C'
  group by
    CohortYear,
    ContractType,
    CashflowNature
)

select
  p.CohortYear,
  p.CashflowNature,
  p.ContractType,
  Premium,
  Expenses,
  Claims,
  Expenses / Premium as ExpenseRatio,
  Claims / Premium as ClaimsRatio,
  (Expenses / Premium) +(Claims / Premium) as CombinedRatio,
  1 +(Expenses / Premium) +(Claims / Premium) as Margin
from
  CTEP p
  inner join CTEE e on (
    p.CohortYear = e.CohortYear
    and p.ContractType = e.ContractType and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortYear = c.CohortYear
    and p.ContractType = c.ContractType and p.CashflowNature=c.CashflowNature
  )
