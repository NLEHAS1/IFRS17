-- Databricks notebook source
with CTE as (select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,a.* from sl1_20201231_v20220529_03.Expenses a  where cashflowsubtype in ('MAI','ACQ','CHE'))

select split(MainProduct,'-')[0] as ProductGroup,a.* from CTE a


-- COMMAND ----------

drop view mi2022.ExpenseTable_1_9

-- COMMAND ----------

create view mi2022.ExpenseTable_1_9 as with CTE as (
  select
     split(MainUnitMainProductSetId, '_') [0] as MainProduct,
    split(MainUnitMainProductSetId, '_') [1] as MainUnit,
    MainUnitMainProductSetId,
    CashflowDate,
    DataSource,
    CashFlowType,
    CashFlowSubType,
    ModelId,
    TransactionCurrency,
    Amount,
    ValuationDate,
    InvoiceDate,
    LossEventDate,
    FromPartyId,
    ToPartyId,
    OriginalFromPartyId,
    OriginalToPartyId,
    S_ACCOUNT,
    S_ENTITY,
    S_DIM1,
    S_DIM4
  from
    sl1_20201231_v20220529_03.Expenses a
  where
    cashflowsubtype in ('MAI', 'ACQ', 'CHE')  and cashflowdate>20180101
),
CTE1 as (
  select
     split(MainUnitMainProductSetId, '_') [0] as MainProduct,
    split(MainUnitMainProductSetId, '_') [1] as MainUnit,
    MainUnitMainProductSetId,
    CashflowDate,
    DataSource,
    CashFlowType,
    CashFlowSubType,
    ModelId,
    TransactionCurrency,
    Amount,
    ValuationDate,
    InvoiceDate,
    LossEventDate,
    FromPartyId,
    ToPartyId,
    OriginalFromPartyId,
    OriginalToPartyId,
    S_ACCOUNT,
    S_ENTITY,
    S_DIM1,
    S_DIM4
  from
    sl1_20200930_v20220529_03.Expenses a
  where
    cashflowsubtype in ('MAI', 'ACQ', 'CHE') and cashflowdate>20180101
),
CTE2 as (
  select
     split(MainUnitMainProductSetId, '_') [0] as MainProduct,
    split(MainUnitMainProductSetId, '_') [1] as MainUnit,
    MainUnitMainProductSetId,
    CashflowDate,
    DataSource,
    CashFlowType,
    CashFlowSubType,
    ModelId,
    TransactionCurrency,
    Amount,
    ValuationDate,
    InvoiceDate,
    LossEventDate,
    FromPartyId,
    ToPartyId,
    OriginalFromPartyId,
    OriginalToPartyId,
    S_ACCOUNT,
    S_ENTITY,
    S_DIM1,
    S_DIM4
  from
    sl1_20200630_v20220529_03.Expenses a
  where
    cashflowsubtype in ('MAI', 'ACQ', 'CHE') and cashflowdate>20180101
),
CTE3 as (
  select
     split(MainUnitMainProductSetId, '_') [0] as MainProduct,
    split(MainUnitMainProductSetId, '_') [1] as MainUnit,
    MainUnitMainProductSetId,
    CashflowDate,
    DataSource,
    CashFlowType,
    CashFlowSubType,
    ModelId,
    TransactionCurrency,
    Amount,
    ValuationDate,
    InvoiceDate,
    LossEventDate,
    FromPartyId,
    ToPartyId,
    OriginalFromPartyId,
    OriginalToPartyId,
    S_ACCOUNT,
    S_ENTITY,
    S_DIM1,
    S_DIM4
  from
    sl1_20200331_v20220529_03.Expenses a
  where
    cashflowsubtype in ('MAI', 'ACQ', 'CHE') and cashflowdate>20180101
),
CTE4 as (
  select
     split(MainUnitMainProductSetId, '_') [0] as MainProduct,
    split(MainUnitMainProductSetId, '_') [1] as MainUnit,
    MainUnitMainProductSetId,
    CashflowDate,
    DataSource,
    CashFlowType,
    CashFlowSubType,
    ModelId,
    TransactionCurrency,
    Amount,
    ValuationDate,
    InvoiceDate,
    LossEventDate,
    FromPartyId,
    ToPartyId,
    OriginalFromPartyId,
    OriginalToPartyId,
    S_ACCOUNT,
    S_ENTITY,
    S_DIM1,
    S_DIM4
  from
    sl1_20191231_v20220529_03.Expenses a
  where
    cashflowsubtype in ('MAI', 'ACQ', 'CHE') and cashflowdate>20180101
),
FxRates as (
  select
    distinct ValuationDate,
    ValuationDate as EffectFromDate,
    ValuationDate as EffectToDate,
    CalculationEntity,
    'EUR' as FromCurrency,
    'EUR' as ToCurrency,
    1 as ConversionRate,
    ConversionType,
    DataSource,
    Context
  from
    db_202012_20220610_000047_502.fxrates
  union all
  select
    *
  from
    db_202012_20220610_000047_502.fxrates
),
FxRates1 as (
  select
    distinct ValuationDate,
    ValuationDate as EffectFromDate,
    ValuationDate as EffectToDate,
    CalculationEntity,
    'EUR' as FromCurrency,
    'EUR' as ToCurrency,
    1 as ConversionRate,
    ConversionType,
    DataSource,
    Context
  from
    db_202009_20220610_000047_501.fxrates
  union all
  select
    *
  from
    db_202009_20220610_000047_501.fxrates
),
FxRates2 as (
  select
    distinct ValuationDate,
    ValuationDate as EffectFromDate,
    ValuationDate as EffectToDate,
    CalculationEntity,
    'EUR' as FromCurrency,
    'EUR' as ToCurrency,
    1 as ConversionRate,
    ConversionType,
    DataSource,
    Context
  from
    db_202006_20220610_000047_500.fxrates
  union all
  select
    *
  from
    db_202006_20220610_000047_500.fxrates
),
FxRates3 as (
  select
    distinct ValuationDate,
    ValuationDate as EffectFromDate,
    ValuationDate as EffectToDate,
    CalculationEntity,
    'EUR' as FromCurrency,
    'EUR' as ToCurrency,
    1 as ConversionRate,
    ConversionType,
    DataSource,
    Context
  from
    db_202003_20220610_000047_499.fxrates
  union all
  select
    *
  from
    db_202003_20220610_000047_499.fxrates
),
FxRates4 as (
  select
    distinct ValuationDate,
    ValuationDate as EffectFromDate,
    ValuationDate as EffectToDate,
    CalculationEntity,
    'EUR' as FromCurrency,
    'EUR' as ToCurrency,
    1 as ConversionRate,
    ConversionType,
    DataSource,
    Context
  from
    db_201912_20220610_000047_498.fxrates
  union all
  select
    *
  from
    db_201912_20220610_000047_498.fxrates
)
select
  split(MainProduct, '-') [0] as ProductGroup,
  split(MainUnit, '-') [0] as UnitGroup,
  to_date(cast(Left(CashflowDate, 6) as string), 'yyyyMM') as CashflowDate,
  to_date(cast(a.valuationDAte as string), 'yyyyMMdd') as ValuationDate,
  MainUnitMainProductSetId,
  a.DataSource,
  MainProduct,
  MainUnit,
  CashFlowType,
  CashFlowSubType,
  ModelId,
  TransactionCurrency,
  Amount,
  InvoiceDate,
  LossEventDate,
  FromPartyId,
  ToPartyId,
  OriginalFromPartyId,
  OriginalToPartyId,
  S_ACCOUNT,
  S_ENTITY,
  S_DIM1,
  S_DIM4,
  CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When TransactionCurrency = 'EUR' then Amount
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then Amount * e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then Amount * f.ConversionRate ---> Actual Cashflows
    Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR
from
  CTE a
  LEFT JOIN FxRates e on (
    TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  LEFT JOIN FxRates f on (
    TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and CashflowDate = f.EffectToDate
  ) ---> Actual Cashflows
Union all
select
  split(MainProduct, '-') [0] as ProductGroup,
  split(MainUnit, '-') [0] as UnitGroup,
  to_date(cast(Left(CashflowDate, 6) as string), 'yyyyMM') as CashflowDate,
  to_date(cast(a.valuationDAte as string), 'yyyyMMdd') as ValuationDate,
  MainUnitMainProductSetId,
  a.DataSource,
  MainProduct,
  MainUnit,
  CashFlowType,
  CashFlowSubType,
  ModelId,
  TransactionCurrency,
  Amount,
  InvoiceDate,
  LossEventDate,
  FromPartyId,
  ToPartyId,
  OriginalFromPartyId,
  OriginalToPartyId,
  S_ACCOUNT,
  S_ENTITY,
  S_DIM1,
  S_DIM4,
  CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When TransactionCurrency = 'EUR' then Amount
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then Amount * e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then Amount * f.ConversionRate ---> Actual Cashflows
    Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR
from
  CTE1 a
  LEFT JOIN FxRates e on (
    TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  LEFT JOIN FxRates f on (
    TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and CashflowDate = f.EffectToDate
  ) ---> Actual Cashflows
Union all
select
  split(MainProduct, '-') [0] as ProductGroup,
  split(MainUnit, '-') [0] as UnitGroup,
  to_date(cast(Left(CashflowDate, 6) as string), 'yyyyMM') as CashflowDate,
  to_date(cast(a.valuationDAte as string), 'yyyyMMdd') as ValuationDate,
  MainUnitMainProductSetId,
  a.DataSource,
  MainProduct,
  MainUnit,
  CashFlowType,
  CashFlowSubType,
  ModelId,
  TransactionCurrency,
  Amount,
  InvoiceDate,
  LossEventDate,
  FromPartyId,
  ToPartyId,
  OriginalFromPartyId,
  OriginalToPartyId,
  S_ACCOUNT,
  S_ENTITY,
  S_DIM1,
  S_DIM4,
  CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When TransactionCurrency = 'EUR' then Amount
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then Amount * e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then Amount * f.ConversionRate ---> Actual Cashflows
    Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR
from
  CTE2 a
  LEFT JOIN FxRates e on (
    TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  LEFT JOIN FxRates f on (
    TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and CashflowDate = f.EffectToDate
  ) ---> Actual Cashflows
Union all
select
  split(MainProduct, '-') [0] as ProductGroup,
  split(MainUnit, '-') [0] as UnitGroup,
  to_date(cast(Left(CashflowDate, 6) as string), 'yyyyMM') as CashflowDate,
  to_date(cast(a.valuationDAte as string), 'yyyyMMdd') as ValuationDate,
  MainUnitMainProductSetId,
  a.DataSource,
  MainProduct,
  MainUnit,
  CashFlowType,
  CashFlowSubType,
  ModelId,
  TransactionCurrency,
  Amount,
  InvoiceDate,
  LossEventDate,
  FromPartyId,
  ToPartyId,
  OriginalFromPartyId,
  OriginalToPartyId,
  S_ACCOUNT,
  S_ENTITY,
  S_DIM1,
  S_DIM4,
  CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When TransactionCurrency = 'EUR' then Amount
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then Amount * e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then Amount * f.ConversionRate ---> Actual Cashflows
    Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR
from
  CTE3 a
  LEFT JOIN FxRates e on (
    TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  LEFT JOIN FxRates f on (
    TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and CashflowDate = f.EffectToDate
  ) ---> Actual Cashflows
Union all
select
  split(MainProduct, '-') [0] as ProductGroup,
  split(MainUnit, '-') [0] as UnitGroup,
  to_date(cast(Left(CashflowDate, 6) as string), 'yyyyMM') as CashflowDate,
  to_date(cast(a.valuationDAte as string), 'yyyyMMdd') as ValuationDate,
  MainUnitMainProductSetId,
  a.DataSource,
  MainProduct,
  MainUnit,
  CashFlowType,
  CashFlowSubType,
  ModelId,
  TransactionCurrency,
  Amount,
  InvoiceDate,
  LossEventDate,
  FromPartyId,
  ToPartyId,
  OriginalFromPartyId,
  OriginalToPartyId,
  S_ACCOUNT,
  S_ENTITY,
  S_DIM1,
  S_DIM4,
  CASE
    When TransactionCurrency = 'EUR' then e.ConversionRate
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When TransactionCurrency = 'EUR' then Amount
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate > e.ValuationDate
    ) then Amount * e.ConversionRate ---> Future Cashflows
    When (
      TransactionCurrency != 'EUR'
      and CashflowDate <= f.ValuationDate
    ) then Amount * f.ConversionRate ---> Actual Cashflows
    Else Amount * e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR
from
  CTE4 a
  LEFT JOIN FxRates e on (
    TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  LEFT JOIN FxRates f on (
    TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and CashflowDate = f.EffectToDate
  ) ---> Actual Cashflows

-- COMMAND ----------

select * from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select * from db_202012_20220610_000047_502.fxrates where effecttodate='20151231' and fromcurrency='AUD' and tocurrency='EUR'

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select distinct valuationdate from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9 

-- COMMAND ----------

with CTE as (select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,a.* from sl1_20201231_v20220529_03.Expenses a  where cashflowsubtype in ('MAI','ACQ','CHE'))

select split(MainProduct,'-')[0] as ProductGroup,a.* from CTE a where mainunit='GLB-AUS' and mainproduct='CI-ST' and CAshflowsubtype='ACQ' order by cashflowdate desc


-- COMMAND ----------

select distinct modelid from db_202012_20220610_000047_502.cashflows

-- COMMAND ----------

select distinct modelid,cashflowsubtype from db_202012_20220610_000047_502.cashflows a left join db_202012_20220610_000047_502.datedistributions b on a.datedistributionid=b.datedistributionid  where cashflowsubtype in ('MAI','ACQ','CHE') and cashflowdate<=b.ValuationDate

-- COMMAND ----------

select distinct modelid,cashflowsubtype from db_202012_20220610_000047_502.cashflows a left join db_202012_20220610_000047_502.datedistributions b on a.datedistributionid=b.datedistributionid  where cashflowsubtype in ('MAI','ACQ','CHE') and cashflowdate>b.ValuationDate

-- COMMAND ----------

select distinct cashflowdate from mi2022.ExpenseTable_1_9 order by cashflowdate asc

-- COMMAND ----------

-- MAGIC %md # Fixing Joins

-- COMMAND ----------

with CTE as (select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,a.* from sl1_20201231_v20220529_03.Expenses a  where cashflowsubtype in ('MAI','ACQ','CHE'))

select split(MainProduct,'-')[0] as ProductGroup,a.* from CTE a


-- COMMAND ----------

select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,a.* from sl1_20201231_v20220529_03.Expenses a  where cashflowsubtype in ('MAI','ACQ','CHE') and MainUnitMainProductSetId like 'RE%'

-- COMMAND ----------

with CTE (select a.*,
case
When MainUnitMainProductSetId like 'CI%' then (split(MainUnitMainProductSetId,'_')[0])
When MainUnitMainProductSetId like 'RE%' then trim('-',left(MainUnitMainProductSetId,charindex('-', MainUnitMainProductSetId,4)))
else (split(MainUnitMainProductSetId,'-')[0])
end as ProductGroup



from sl1_20201231_v20220529_03.Expenses a)

select distinct ProductGroup from CTE

-- COMMAND ----------

with CTE (select  a.*,
case
When MainProduct like 'CI%' then (split(MainProduct,'_')[0])
When MainProduct like 'RE%' then trim('-',left(MainProduct,charindex('-', MainProduct,4)))
else (split(MainProduct,'-')[0])
end as ProductGroup1 from mireporting.CashflowsMetrics_1_9 a)

select distinct ProductGroup1 from CTE

-- COMMAND ----------

desc mireporting.CashflowsMetrics_1_9 
