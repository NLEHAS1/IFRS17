-- Databricks notebook source
drop view MI2022.ExpenseAnalysisAggV2

-- COMMAND ----------

Create View MI2022.ExpenseAnalysisAggV2 as
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
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CustomCashflowDate,
    Left(a.ContractIssueDate, 4) as CohortYear,
    Sum(amount) as Amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows2_201912_20220413_133443_206 b on a.contractid = b.contractid
    left join DateDistributionsTrans c on b.DateDistributionId = c.DateDistributionId
  where
    Left(a.ContractIssueDate, 4) > 2000
  group by
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CustomCashflowDate,
    CohortYear
),

CTE2 as (
  select
    a.*,
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

CTE3 as (
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
    End as ContractType
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.Contractid = b.contractid
      and GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001 and intra_group_elimination_flg is null
    )
),
CTEP as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Premium
  from
    CTECT
  where
    Cashflowtype = 'P'
  group by
    CohortYear,
    ContractType
),
CTEE as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Expenses
  from
    CTECT
  where
    Cashflowtype = 'E'
  group by
    CohortYear,
    ContractType
),
CTEC as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Claims
  from
    CTECT
  where
    Cashflowtype = 'C'
  group by
    CohortYear,
    ContractType
)
select
  p.CohortYear,
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
    and p.ContractType = e.ContractType
  )
  inner join CTEC c on (
    p.CohortYear = c.CohortYear
    and p.ContractType = c.ContractType
  )

-- COMMAND ----------

select * from MI2022.ExpenseAnalysisAggV2 order by cohortyear desc

-- COMMAND ----------

select * from MI2022.ExpenseAnalysisAggV2 

-- COMMAND ----------

select sum(customValue) from MI2022.StartingPointReports_201912_20220413_133443_206 where cashflowtype='C' and Contracttype='Direct Contract' and cohortyear=2020 and calculationentity=001

-- COMMAND ----------

select sum(customValue) from MI2022.StartingPointReports_201912_20220413_133443_206 where cashflowtype='E' and Contracttype='Direct Contract' and cohortyear=2020 and calculationentity=001

-- COMMAND ----------

select sum(customValue) from MI2022.StartingPointReports_201912_20220413_133443_206 where cashflowtype='P' and Contracttype='Direct Contract' and cohortyear=2020 and calculationentity=001

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where cashflowtype='P' and Contracttype='Direct Contract' and cohortyear=2020 and calculationentity=001

-- COMMAND ----------

select * from MI2022.FXRates_201912_20220413_133443_206 where fromcurrency='MUR' and tocurrency='EUR' and effecttodate=20191231

-- COMMAND ----------

-- MAGIC %md investigate the AED to EUR conversionrate at 20000127

-- COMMAND ----------

select * from MI2022.contracts_201912_20220413_133443_206 where contractid='SYM / 102226 / 19940701'

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206  where contractid='SYM / 102226 / 19940701'

-- COMMAND ----------

select * from MI2022.datedistributions_201912_20220413_133443_206 where datedistributionid=97678755

-- COMMAND ----------

select * from MI2022.FxRates3_201912_20220413_133443_206 where Tocurrency='EUR' and fromcurrency='AED' and effecttodate=20191231

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='SYM / 102226 / 19940701'

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='SYM / 102226 / 19940701' and cashflowid='30357359511473376'

-- COMMAND ----------

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
    select * from DateDistributionsTrans where DateDistributionId=97678755

-- COMMAND ----------

-- MAGIC %md The code below show us if any TransactionCurrency left without CoversionRate

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
    a.ContractId,
    b.cashflowid,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CustomCashflowDate,
    Left(a.ContractIssueDate, 4) as CohortYear,
    amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid = b.contractid
    left join DateDistributionsTrans c on b.DateDistributionId = c.DateDistributionId
),

CTE2 as (
  select
    a.*,
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
)

select * from CTE2 where AmountEUR =Amount and TransactionCurrency!='EUR' and amount!=0

-- COMMAND ----------

select * from MI2022.FxRates3_201912_20220413_133443_206 where fromcurrency='ZAR' and ToCurrency='EUR' and effecttodate between 20031111 and 20060101

-- COMMAND ----------

select * from MI2022.FxRates3_201912_20220413_133443_206 where fromcurrency='AUD' and ToCurrency='EUR' and effecttodate=20000620

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206 where contractid='SYM / 485530 / 20150401' and cashflowid='8916262262961367656'

-- COMMAND ----------

select distinct valuationdate from MI2022.fxrates_201912_20220413_133443_206

-- COMMAND ----------

select distinct valuationdate from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select * from  MI2022.contract_type_201912_20220413_133443_206 where contractid='SYM / 102226 / 19940701' 

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
    a.ContractId,
    b.cashflowid,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CustomCashflowDate,
    Left(a.ContractIssueDate, 4) as CohortYear,
    amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid = b.contractid
    left join DateDistributionsTrans c on b.DateDistributionId = c.DateDistributionId
),

CTE2 as (
  select
    a.*,
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
)

select * from CTE2 where
