-- Databricks notebook source
-- MAGIC %md --Features of the data:
-- MAGIC 
-- MAGIC 1. We are processing only contracts that have contract issue date greater than 2015
-- MAGIC 2. We are processing only direct contracts
-- MAGIC 3. We are processing only CI product
-- MAGIC 4. We are calculating Eur totals at Atradius NV level where InsurerId = FromPartyId we will have negative amount otherwise we will have positive amount
-- MAGIC 5. Cashflows table have duplicates that had been deal with by taking the distinct values
-- MAGIC 6. Datedistributions have nulls that had been deal with by replacing nulls with zero
-- MAGIC 7. Amounts had been converted to Euro where Actuals' coversion rates calculated at cashflowdate, while futures' conversion rate calculated at valuation date
-- MAGIC 8. Other DQ issues such as missing values or referntial integrity have not been deal with
-- MAGIC 9. Schemas (1.4 release): 1.201912_20220413_133443_206, 2.202003_20220412_081208_191, 3.202006_20220412_143707_198, 4.202009_20220413_195153_208, 5.202012_20220413_140845_207

-- COMMAND ----------

DROP VIEW mi2022.expenseanalysisaggV2

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
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
  where Left(a.ContractIssueDate, 4) >= 2015 and c.Datasource!='RE2021' and 
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.MainUnit,
  p.MainProduct,
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
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct
  )

-- COMMAND ----------

-- MAGIC %md All Analysis

-- COMMAND ----------

drop view mi2022.expenseanalysis

-- COMMAND ----------

-- MAGIC %md #201912_20220413_133443_206

-- COMMAND ----------

select count(*) from mi2022.expenseanalysis_201912_20220413_133443_206 

-- COMMAND ----------

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

CTE1 as (
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(UnderwritingMonth as string), 'yyyyMM') as UnderwritingMonth,
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate
  )

-- COMMAND ----------

select * from mi2022.expenseanalysis_201912_20220413_133443_206 order by UnderwritingMonth asc

-- COMMAND ----------

show tables in mi2022

-- COMMAND ----------

create view MI2022.cashflows2_202003_20220412_081208_191 as
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
from MI2022.cashflows_202003_20220412_081208_191

-- COMMAND ----------

select * from mi2022.FxRates_202003_20220412_081208_191 

-- COMMAND ----------

drop table mireporting.FxRates2_202003_20220412_081208_191

-- COMMAND ----------

create table mireporting.FxRates22_202003_20220412_081208_191 (
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

insert into mireporting.FxRates22_202003_20220412_081208_191  values (
20200331,20200331,20200331,'*','EUR','EUR',1,'None','None','IFRS17'
)

-- COMMAND ----------

create view mi2022.FxRates3_202003_20220412_081208_191 as 
select * from mireporting.FxRates22_202003_20220412_081208_191 
union all 
select * from MI2022.FxRates_202003_20220412_081208_191

-- COMMAND ----------

-- MAGIC %md #202003_20220412_081208_191

-- COMMAND ----------

drop view mi2022.expenseanalysis_202003_20220412_081208_191

-- COMMAND ----------

Create view mi2022.expenseanalysis_202003_20220412_081208_191 as 
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

CTE1 as (
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
    MI2022.contracts_202003_20220412_081208_191 a
  LEFT JOIN MI2022.cashflows2_202003_20220412_081208_191 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202003_20220412_081208_191 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select 
  a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(UnderwritingMonth as string), 'yyyyMM') as UnderwritingMonth,
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
  TopartyName
,
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate
  )

-- COMMAND ----------

select * from mi2022.expenseanalysis_202003_20220412_081208_191

-- COMMAND ----------

drop view mi2022.ExpenseAnalysis

-- COMMAND ----------

create view mi2022.ExpenseAnalysis as 
select * from mi2022.expenseanalysis_201912_20220413_133443_206
union all 
select * from mi2022.expenseanalysis_202003_20220412_081208_191

-- COMMAND ----------

-- select distinct to_date(cast((valuationdate) as string), 'yyyyMM') as valuationdate from mi2022.FxRates_202003_20220412_081208_191 

--   to_date(cast(a.CoverEndDate as string), 'yyyyMMdd') as UnderwritingMonth,
--   Left(a.ContractIssueDate, 6) as UnderwritingMonth,

with CTE as
(select distinct Left(EffectToDate, 6) as EffectToDate from mi2022.FxRates_202003_20220412_081208_191 ) 

select to_date(cast(EffectToDate as string), 'yyyyMM') as valuationdate from CTE

-- COMMAND ----------

select * from MI2022.datedistributions_202003_20220412_081208_191

-- COMMAND ----------

-- MAGIC %md #202006_20220412_143707_198

-- COMMAND ----------

Create view mi2022.expenseanalysis_202006_20220412_143707_198 as 
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

CTE1 as (
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
    MI2022.contracts_202006_20220412_143707_198 a
  LEFT JOIN MI2022.cashflows2_202006_20220412_143707_198 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202006_20220412_143707_198 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select 
  a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(UnderwritingMonth as string), 'yyyyMM') as UnderwritingMonth,
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
  TopartyName
,
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate
  )

-- COMMAND ----------

create view mi2022.ExpenseAnalysis as 
select * from mi2022.expenseanalysis_201912_20220413_133443_206
union all 
select * from mi2022.expenseanalysis_202003_20220412_081208_191
union all
SELECT * FROM mi2022.expenseanalysis_202006_20220412_143707_198

-- COMMAND ----------

-- MAGIC %md #202009_20220413_195153_208

-- COMMAND ----------

Create view mi2022.expenseanalysis_202009_20220413_195153_208 as 
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

CTE1 as (
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
    MI2022.contracts_202009_20220413_195153_208 a
  LEFT JOIN MI2022.cashflows2_202009_20220413_195153_208 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202009_20220413_195153_208 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select 
  a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(UnderwritingMonth as string), 'yyyyMM') as UnderwritingMonth,
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
  TopartyName
,
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate
  )

-- COMMAND ----------

drop view mi2022.ExpenseAnalysis 

-- COMMAND ----------

create view mi2022.ExpenseAnalysis as 
select * from mi2022.expenseanalysis_201912_20220413_133443_206
union all 
select * from mi2022.expenseanalysis_202003_20220412_081208_191
union all
SELECT * FROM mi2022.expenseanalysis_202006_20220412_143707_198
union all
select * from mi2022.expenseanalysis_202009_20220413_195153_208

-- COMMAND ----------

-- MAGIC %md #202012_20220413_140845_207

-- COMMAND ----------

Create view mi2022.expenseanalysis_202012_20220413_140845_207 as 
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

CTE1 as (
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
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
    MI2022.contracts_202012_20220413_140845_207 a
  LEFT JOIN MI2022.cashflows2_202012_20220413_140845_207 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_202012_20220413_140845_207 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
),

CTE2 as (
select 
  a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(UnderwritingMonth as string), 'yyyyMM') as UnderwritingMonth,
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
  TopartyName
,
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
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEE as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
),

CTEC as (
  select
    UnderwritingMonth,
    MainUnit,
    MainProduct,
    ValuationDate,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    UnderwritingMonth,
    MainUnit,
    ValuationDate,
    MainProduct
)

select
  p.UnderwritingMonth,
  p.ValuationDate,
  p.MainUnit,
  p.MainProduct,
  Premium,
  Expenses,
  Claims
from
  CTEP p
  inner join CTEE e on (
    p.UnderwritingMonth = e.UnderwritingMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate
  )
  inner join CTEC c on (
    p.UnderwritingMonth = c.UnderwritingMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate
  )

-- COMMAND ----------

drop view mi2022.ExpenseAnalysis

-- COMMAND ----------

create view mi2022.ExpenseAnalysis as 
select * from mi2022.expenseanalysis_201912_20220413_133443_206
union all 
select * from mi2022.expenseanalysis_202003_20220412_081208_191
union all
SELECT * FROM mi2022.expenseanalysis_202006_20220412_143707_198
union all
select * from mi2022.expenseanalysis_202009_20220413_195153_208
union all
select * from mi2022.expenseanalysis_202012_20220413_140845_207
