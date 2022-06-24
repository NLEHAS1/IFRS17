-- Databricks notebook source
drop view MI2022.ExpensesDistribInti

-- COMMAND ----------

Create view MI2022.ExpensesDistribInti as 
-- CTE for extracting the aggregate figures
with Agg as (
  select
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    GroupingKey,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    Entity,
    TopartyEntityId,
    DirectContractFlg,
    ReinContractFlg,
    IntraGroupEliminationFlg,
    CalculationEntity,
    ProfitabilityClass,
    sum(CustomValue) as CustomValue
  from
    MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab
  where
     CalculationEntity = '001'
  group by
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    GroupingKey,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    Entity,
    TopartyEntityId,
    DirectContractFlg,
    ReinContractFlg,
    IntraGroupEliminationFlg,
    CalculationEntity,
    ProfitabilityClass
),

--CTE to calculate the sum of premium of contracts
Prbased as(
  select
    ContractId,
    sum(CustomValue) as PremiumValue
  from
    MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab
  where
    cashflowtype = 'P'
    and CalculationEntity = '001'
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
  COALESCE(c.ExpenseRatio,0) as ExpenseRatio,
  COALESCE(c.ClaimsRatio,0) as ClaimsRatio,
  COALESCE(b.PremiumValue,0) as PremiumValue
from
  Agg a
  left join Prbased b on a.contractid = b.contractid
  left join AggRatio c on a.contractid = c.contractid

-- COMMAND ----------

drop view MI2022.ExpensesDistrib

-- COMMAND ----------

-- MAGIC %md Open question: How to deals with Contracts that has no Premium or Expense or claims

-- COMMAND ----------

Create view  MI2022.ExpensesDistrib as 
select a.*,
ExpenseRatio+ClaimsRatio as CombinedRatio,
1+ExpenseRatio+ClaimsRatio as Margin
from MI2022.ExpensesDistribInti a

-- COMMAND ----------

-- MAGIC %md Run the code below to make sure the code modification work ---> PremiumValue isn't available check why

-- COMMAND ----------

select * from MI2022.ExpensesDistrib where contractid='SYM / 352214 / 20190101' 

-- COMMAND ----------

select * from MI2022.ExpensesDistrib where contractid='SYM / 352214 / 20190101' 

-- COMMAND ----------

select sum(customvalue)  from MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab where contractid='SYM / 352214 / 20190101' and cashflowtype='E'

-- COMMAND ----------

with Agg as (
  select
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    GroupingKey,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    Entity,
    TopartyEntityId,
    DirectContractFlg,
    ReinContractFlg,
    IntraGroupEliminationFlg,
    CalculationEntity,
    ProfitabilityClass,
    sum(CustomValue) as CustomValue
  from
    MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab
  where
     CalculationEntity = '001'
  group by
    ValuationDate,
    ContractId,
    PolicyId,
    InsurerId,
    InsuredId,
    CohortYear,
    MainProduct,
    Unit,
    MainUnit,
    GroupingKey,
    CashFlowType,
    CashFlowSubType,
    FromPartyId,
    ToPartyId,
    TransactionCurrency,
    CashflowNature,
    FrompartyEntityId,
    Entity,
    TopartyEntityId,
    DirectContractFlg,
    ReinContractFlg,
    IntraGroupEliminationFlg,
    CalculationEntity,
    ProfitabilityClass
)

select * from agg where contractid='SYM / 352214 / 20190101' 

-- COMMAND ----------

with Prbased as(
  select
    ContractId,
    sum(CustomValue) as PremiumValue
  from
    MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab
  where
    cashflowtype = 'P'
    and CalculationEntity = '001'
  group by
    ContractId
)
select * from Prbased where contractid='SYM / 352214 / 20190101' 

-- COMMAND ----------

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
