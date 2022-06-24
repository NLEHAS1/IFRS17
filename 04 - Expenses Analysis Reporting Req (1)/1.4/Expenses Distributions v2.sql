-- Databricks notebook source
drop view MI2022.ExpensesDistribInti_201912_20220413_133443_206

-- COMMAND ----------

Create view MI2022.ExpensesDistribInti_201912_20220413_133443_206 as 
-- CTE for extracting the aggregate figures
with Agg as (
  select
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    InsurerEntityId,
    InsuredEntityId,
    CohortYear,
    UnderwritingMonth,
    MainProduct,
    Unit,
    MainUnit,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    TopartyEntityId,
    sum(CustomValue) as CustomValue
  from
    MI2022.StartingPointReports_201912_20220413_133443_206 --where ContractsDataSource='SYM'
  group by
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    InsurerEntityId,
    InsuredEntityId,
    CohortYear,
    UnderwritingMonth,
    MainProduct,
    Unit,
    MainUnit,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    TopartyEntityId
),

--CTE to calculate the sum of premium of contracts
Prbased as(
  select
    ContractId,
    sum(CustomValue) as PremiumValue
  from
    MI2022.StartingPointReports_201912_20220413_133443_206
  where
     cashflowtype = 'P' --and ContractsDataSource='SYM'
  group by
    ContractId
),

--CTE to calculate Expense and Claims ratios
AggRatio as (
  Select
    *
  from
    (
      select
        a.ContractId,
        Cashflowtype,
        Sum(a.CustomValue / b.PremiumValue) as Ratio
      from
        Agg a
        left join Prbased b on a.contractid = b.contractid
      group by
        a.ContractId,
        Cashflowtype
    ) pivot (
      Max(Ratio) for Cashflowtype in ('E' as ExpenseRatio, 'C' as ClaimsRatio)
    )
)

select
  a.*,
  a.CustomValue / b.PremiumValue as Ratio, -- Here we are calculating ratios
  c.ExpenseRatio,
  c.ClaimsRatio
from
  Agg a
  left join Prbased b on a.contractid = b.contractid
  left join AggRatio c on a.contractid = c.contractid 

-- COMMAND ----------

drop view  MI2022.ExpensesDistrib_201912_20220413_133443_206

-- COMMAND ----------

Create view  MI2022.ExpensesDistrib_201912_20220413_133443_206 as 
select a.*,
ExpenseRatio+ClaimsRatio as CombinedRatio,
1+ExpenseRatio+ClaimsRatio as Margin
from MI2022.ExpensesDistribInti_201912_20220413_133443_206 a where UnderwritingMonth>201502

-- COMMAND ----------

select count(*) from MI2022.ExpensesDistrib_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.ExpensesDistrib_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from MI2022.ExpensesDistrib_201912_20220413_133443_206

-- COMMAND ----------

select count(*) from (
select
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    InsurerEntityId,
    InsuredEntityId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    TopartyEntityId,
    sum(CustomValue) as CustomValue
  from
    MI2022.StartingPointReports_201912_20220413_133443_206 where ContractsDataSource='SYM'
  group by
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    InsurerEntityId,
    InsuredEntityId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    TopartyEntityId)

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

select count(*) from FROM
  MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows_201912_20220413_133443_206 c on 
    a.ContractId = c.ContractId
    LEFT JOIN DateDistributionsTrans d on 
    c.DateDistributionid = d.DateDistributionid where a.Datasource='SYM'

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

select count(*) from FROM
  MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows_201912_20220413_133443_206 c on 
    a.ContractId = c.ContractId
    LEFT JOIN DateDistributionsTrans d on 
    c.DateDistributionid = d.DateDistributionid

-- COMMAND ----------

select * from  MI2022.ExpensesDistrib_201912_20220413_133443_206 where contractid='SYM / 1014190 / 20200101'

-- COMMAND ----------

select * from MI2022.cashflows_201912_20220413_133443_206 where contractid='SYM / 1014190 / 20200101' 

-- COMMAND ----------

select * from MI2022.contracts_201912_20220413_133443_206 a left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid=b.contractid where a.contractid='SYM / 1014190 / 20200101' and Left(a.ContractIssueDate, 6)>201502

-- COMMAND ----------

select * from MI2022.StartingPointReports_201912_20220413_133443_206 where contractid='SYM / 1000001 / 20190701' and UnderwritingMonth>201502

-- COMMAND ----------

with Prbased as(
  select
    ContractId,
    sum(CustomValue) as PremiumValue
  from
    MI2022.StartingPointReports_201912_20220413_133443_206
  where
     cashflowtype = 'P' and ContractsDataSource='SYM'
  group by
    ContractId
)
select * from Prbased where contractid='SYM / 1014190 / 20200101'

-- COMMAND ----------

select a.*,
ExpenseRatio+ClaimsRatio as CombinedRatio,
1+ExpenseRatio+ClaimsRatio as Margin
from MI2022.ExpensesDistribInti_201912_20220413_133443_206 a where contractid='SYM / 124189 / 20161201'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Step 1: all valuation Date
-- MAGIC Totals ratios (total claims/total premium (example)) by Cohort Year 2000 ---> we have to separate Direct and Reinsurance business
-- MAGIC 
-- MAGIC 
-- MAGIC Conditions
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC step2
-- MAGIC total ratios/Cohort Year/Underwriting month/CashflowNature
