-- Databricks notebook source
-- MAGIC %md Reun the code. The code is ready, I'm not sure if to use inner join or left join at the last query between CTEP and Agg, try with left first then inner join

-- COMMAND ----------

Create or replace table mireporting.expenseanalysis_1_5_V2 as 
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
      MI2022.datedistributions_201912_20220522_180045_173
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
  MI2022.cashflows2_201912_20220522_180045_173
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
    MI2022.contracts_201912_20220522_180045_173 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
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
    LEFT JOIN MI2022.FxRates3_201912_20220522_180045_173 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_201912_20220522_180045_173 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

Agg as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct,
    Sum(CustomValue) as CustomValue
  from
    CTE3
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct)

Select 
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.CohortMonth,
    a.CohortYear,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    a.CustomValue,
    coalesce(b.Premium,0) as Premium
    from Agg a left join CTEP b on (a.ValuationDate=b.ValuationDate and a.CohortMonth=b.CohortMonth and a.CohortYear=b.CohortYear and 
    a.MainUnit=b.MainUnit and a.CashflowNature=b.CashflowNature and a.MainProduct=b.MainProduct )
    


-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_1_5_V2

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_1_5_V2

-- COMMAND ----------

select distinct valuationdate12 from mireporting.expenseanalysis_1_5_V2

-- COMMAND ----------

select sum(customvalue) from mireporting.expenseanalysis_1_5_V2 where cashflowtype='P'

-- COMMAND ----------

desc extended mireporting.expenseanalysis_1_5_V2

-- COMMAND ----------

select distinct mainproduct from MI2022.contracts_201912_20220522_180045_173 where Datasource!='RE2021' 

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
      MI2022.datedistributions_201912_20220522_180045_173
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
  MI2022.cashflows2_201912_20220522_180045_173
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
    MI2022.contracts_201912_20220522_180045_173 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220522_180045_173 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
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
    LEFT JOIN MI2022.FxRates3_201912_20220522_180045_173 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_201912_20220522_180045_173 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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


  select
    
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P' and cohortyear='2017-01-01'
 
    


-- COMMAND ----------

select * from mireporting.expenseanalysis_1_5_V2 an
