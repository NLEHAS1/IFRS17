-- Databricks notebook source
drop view mi2022.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

Create view mi2022.expenseanalysis_201912_20220413_133443_206 as 
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_201912_20220413_133443_206
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  )

-- COMMAND ----------

select * from  mi2022.expenseanalysis_201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md #201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md The code below is ready I check with couple of records with V1

-- COMMAND ----------

Create table mireporting.expenseanalysis_201912_20220413_133443_206 as 
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_201912_20220413_133443_206
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  )

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_201912_20220413_133443_206

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 where cohortmonth='2019-07-01' and mainunit='GLB-AUT'

-- COMMAND ----------

select distinct cashflownature from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationdate=20191231

-- COMMAND ----------

-- MAGIC %md #202003_20220412_081208_191

-- COMMAND ----------

Insert into mireporting.expenseanalysis_201912_20220413_133443_206 
select * from (
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
      MI2022.datedistributions_202003_20220412_081208_191
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_202003_20220412_081208_191
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_202003_20220412_081208_191 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    LEFT JOIN MI2022.FxRates3_202003_20220412_081208_191 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_202003_20220412_081208_191 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  ))

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct valuationDAte from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationDate=20200331 and mainunit='LOC-MEX' and cohortmonth='2015-10-01'

-- COMMAND ----------

select distinct cashflownature from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationdate=20200331

-- COMMAND ----------

-- MAGIC %md #202006_20220412_143707_198

-- COMMAND ----------

Insert into mireporting.expenseanalysis_201912_20220413_133443_206 
select * from (
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
      MI2022.datedistributions_202006_20220412_143707_198
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_202006_20220412_143707_198
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_202006_20220412_143707_198 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    LEFT JOIN MI2022.FxRates3_202006_20220412_143707_198 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_202006_20220412_143707_198 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  ))

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct valuationdate from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct cashflownature from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationdate=20200630

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationDate=20200630 and mainunit='LOC-BEL' and cohortmonth='2019-01-01'

-- COMMAND ----------

-- MAGIC %md #202009_20220413_195153_208

-- COMMAND ----------

Insert into mireporting.expenseanalysis_201912_20220413_133443_206 
select * from (
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
      MI2022.datedistributions_202009_20220413_195153_208
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_202009_20220413_195153_208
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_202009_20220413_195153_208 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    LEFT JOIN MI2022.FxRates3_202009_20220413_195153_208 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_202009_20220413_195153_208 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  ))

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct valuationdate from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct cashflownature from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationdate=20200930

-- COMMAND ----------

select distinct mainproduct from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationDate=20200930 and mainunit='LOC-DEU' and cohortmonth='2017-08-01'

-- COMMAND ----------

-- MAGIC %md #202012_20220413_140845_207

-- COMMAND ----------

Insert into mireporting.expenseanalysis_201912_20220413_133443_206 
select * from (
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
      MI2022.datedistributions_202012_20220413_140845_207
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_202012_20220413_140845_207
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_202012_20220413_140845_207 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    LEFT JOIN MI2022.FxRates3_202012_20220413_140845_207 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_202012_20220413_140845_207 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  inner join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  ))

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct valuationdate from mireporting.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

select distinct cashflownature from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationdate=20201231

-- COMMAND ----------

select * from mireporting.expenseanalysis_201912_20220413_133443_206 where valuationDate=20201231 and mainunit='LOC-DEU' and cohortmonth='2019-01-01' and mainproduct='CI-ST'

-- COMMAND ----------

-- MAGIC %md #Validation

-- COMMAND ----------

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
      MI2022.datedistributions_202012_20220413_140845_207
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  MI2022.cashflows2_202012_20220413_140845_207
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_202012_20220413_140845_207 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  CustomCashflowDate,
  CashflowNature,
  InsurerEntityId,
  InsurerName,
  InsuredEntityId,
  InsuredName,
  FrompartyEntityId,
  FrompartyName,
  TopartyEntityId,
  TopartyName,
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
    LEFT JOIN MI2022.FxRates3_202012_20220413_140845_207 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_202012_20220413_140845_207 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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
)

select * from CTE3

-- COMMAND ----------

select * from MI2022.cashflows_202012_20220413_140845_207 where cashflowid='1237321641474721379' and contractid='SYM / 866490 / 20160401'

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220413_140845_207 where datedistributionid=765048129

-- COMMAND ----------

select * from MI2022.FxRates_202012_20220413_140845_207 where fromcurrency='AED' and tocurrency='EUR' and effectfromdate=20190418

-- COMMAND ----------

select count(*) from MI2022.cashflows2_201912_20220522_180045_173
