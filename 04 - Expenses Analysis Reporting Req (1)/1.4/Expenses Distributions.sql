-- Databricks notebook source
use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;

-- COMMAND ----------

select * from MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab where contractid='SYM / 299655 / 20180901'

-- COMMAND ----------

select contractid,count(distinct CashFlowType) from cashflows group by contractid having count(distinct CashFlowType)>2 order by 2 desc

-- COMMAND ----------

desc MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab

-- COMMAND ----------

select * from  MI2022.ExpensesDistribInti

-- COMMAND ----------

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
    contractid in (
      'SYM / 299655 / 20180901',
      --LOC-DEU
      'SYM / 115151 / 20180101',
      --GLB-ITA
      'SYM / 112840 / 20191201',
      --GLB-NOR
      'SYM / 112698 / 20150701',
      --LOC-DEU
      'SYM / 111812 / 20170301',
      --LOC-GBR
      'SYM / 1029256 / 20191001',
      --LOC-FRA
      'SYM / 1017837 / 20190701',
      --GLB-FRA
      'SYM / 1002586 / 20190801',
      --LOC-GRC
      'SYM / 1014129 / 20190701',
      --LOC-CHE
      'CYC / 30143737 / 20191001',
      -- LOC-ESP
      '1415@-1@15763202@21957770',
      --Extragroup
      'SYM / 1000101 / 20190701' --LOC-NLD --12
    )
    and CalculationEntity = '001'
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
    contractid in (
      'SYM / 299655 / 20180901',
      --LOC-DEU
      'SYM / 115151 / 20180101',
      --GLB-ITA
      'SYM / 112840 / 20191201',
      --GLB-NOR
      'SYM / 112698 / 20150701',
      --LOC-DEU
      'SYM / 111812 / 20170301',
      --LOC-GBR
      'SYM / 1029256 / 20191001',
      --LOC-FRA
      'SYM / 1017837 / 20190701',
      --GLB-FRA
      'SYM / 1002586 / 20190801',
      --LOC-GRC
      'SYM / 1014129 / 20190701',
      --LOC-CHE
      'CYC / 30143737 / 20191001',
      -- LOC-ESP
      '1415@-1@15763202@21957770',
      --Extragroup
      'SYM / 1000101 / 20190701' --LOC-NLD
    )
    and cashflowtype = 'P'
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
  c.ExpenseRatio,
  c.ClaimsRatio
from
  Agg a
  left join Prbased b on a.contractid = b.contractid
  left join AggRatio c on a.contractid = c.contractid

-- COMMAND ----------

drop view MI2022.ExpensesDistrib

-- COMMAND ----------

Create view  MI2022.ExpensesDistrib as 
select a.*,
ExpenseRatio+ClaimsRatio as CombinedRatio,
1+ExpenseRatio+ClaimsRatio as Margin
from MI2022.ExpensesDistribInti a

-- COMMAND ----------

select * from  MI2022.ExpensesDistrib 

-- COMMAND ----------

-- MAGIC %md Define the schema

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl2")
