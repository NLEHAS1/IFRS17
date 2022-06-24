-- Databricks notebook source
-- MAGIC %md #201912_20220413_133443_206

-- COMMAND ----------

-- MAGIC %md --Features of the data:
-- MAGIC 
-- MAGIC 1. We are processing only contracts that have contract issue date greater than 2015
-- MAGIC 2. We are processing only direct contracts
-- MAGIC 3. We are processing only CI product
-- MAGIC 4. We are calculating Eur totals at Atradius NV level where InsurerId = FromPartyId we will have negative amount otherwise we will have positive amount

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
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  a.InsuredId,
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
  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB') --fix
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  InsuredId,
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
) select * from CTE3 where contractid='SYM / 471776 / 20160901' and cashflowid='28683043099182656'

-- COMMAND ----------

select * from MI2022.FxRates_201912_20220413_133443_206 where fromcurrency='PLN' and tocurrency='EUR' and effecttodate=20170808

-- COMMAND ----------

select * from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid 
left join MI2022.datedistributions_201912_20220413_133443_206 c on b.datedistributionid=c.datedistributionid
where a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB') and Left(ContractIssueDate, 4) >= 2015 and TransactionCurrency!='EUR' and cashflowdate<=c.valuationdate


-- COMMAND ----------

select * from MI2022.Entities_201912_20220413_133443_206

-- COMMAND ----------

select * from  MI2022.datedistributions_201912_20220413_133443_206 where DateDistributionId=-583272462

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
)
select * from DateDistributionsTrans where DateDistributionId=-583272462

-- COMMAND ----------

select c.contractid,c.cashflowid,count(*) from MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows2_201912_20220413_133443_206 c on (a.ContractId = c.ContractId)  where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB') group by c.contractid,c.cashflowid having count(*)>1 order by 3 desc

-- COMMAND ----------

select distinct datasource from MI2022.cashflows_201912_20220413_133443_206

-- COMMAND ----------

select 
a.contractid, 
cashflowid ,
FromPartyId,
ToPartyId, 
CashFlowType,
CashFlowSubType,
count(*)
from
MI2022.contracts_202003_20220412_081208_191 a
  LEFT JOIN MI2022.cashflows2_202003_20220412_081208_191 b on (a.ContractId = b.ContractId)
where Left(a.ContractIssueDate, 4) >= 2015 and a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB')
group by 
a.contractid, 
cashflowid ,
FromPartyId,
ToPartyId, 
CashFlowType,
CashFlowSubType
having count(*)>1
order by 7 desc

-- COMMAND ----------

select * from MI2022.cashflows2_201912_20220413_133443_206 where contractid='1867@-1@18854881@16780821' and cashflowid='8916262269403778161' and frompartyid='18854881' and topartyid='16780821' and cashflowtype='E' and cashflowsubtype='ESS'

-- COMMAND ----------

select distinct a.datasource,b.datasource from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid

-- COMMAND ----------

-- MAGIC %md #202003_20220412_081208_191

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
)

select * from CTE3 where contractid='CYC / 30131267 / 20170101' and cashflowid='4456801463242347687'

-- COMMAND ----------

select * from MI2022.contracts_202003_20220412_081208_191 a left join MI2022.cashflows_202003_20220412_081208_191 b on a.contractid=b.contractid 
left join MI2022.datedistributions_202003_20220412_081208_191 c on b.datedistributionid=c.datedistributionid
where a.Datasource!='RE2021' AND MainProduct in ('CI-ST','CI-MTB') and Left(ContractIssueDate, 4) >= 2015  and cashflowdate>c.valuationdate and weight=0


-- COMMAND ----------

select * from MI2022.FxRates3_202003_20220412_081208_191  where fromcurrency='JPY' and tocurrency='EUR' and effecttodate=20200331

