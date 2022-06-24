-- Databricks notebook source
-- MAGIC %md #Validation 1

-- COMMAND ----------

-- MAGIC %md The Calculation Stands

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Expenses where MainUnitMainProductSetId='CI-ST_LOC-ROU' and cashflowdate=20201231 and cashflowsubtype='ACQ'

-- COMMAND ----------

select * from db_202012_20220610_000047_502.fxrates where effecttodate=20201231 and fromcurrency='RON' and tocurrency='EUR'

-- COMMAND ----------

select
  a.*,
  b.*,
  c.*,
  d.*,
  Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount * ConversionRate * -1
    Else Amount * ConversionRate
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  left join db_202012_20220610_000047_502.Fxrates d on (
    TransactionCurrency = FromCurrency
    and ToCurrency = 'EUR'
    and CashflowDate = EffectToDate
  )
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-ROU'
  and cashflowdate = 20201231
  and cashflowsubtype = 'ACQ'

-- COMMAND ----------

select count(distinct a.contractid) from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  left join db_202012_20220610_000047_502.Fxrates d on (TransactionCurrency = FromCurrency and ToCurrency = 'EUR' and CashflowDate = EffectToDate)
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-ROU'
  and cashflowdate = 20201231
  and cashflowsubtype = 'ACQ'

-- COMMAND ----------

-- MAGIC %md #Validation 2

-- COMMAND ----------

-- MAGIC %md The result show that the modeling of my data isn't correct

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Expenses where MainUnitMainProductSetId='CI-MTB_LOC-DEU' and cashflowdate=20190630 and cashflowsubtype='ACQ'

-- COMMAND ----------

select sum(amount) from sl1_20201231_v20220529_03.Expenses where MainUnitMainProductSetId='CI-MTB_LOC-DEU' and cashflowdate=20190630 and cashflowsubtype='ACQ'

-- COMMAND ----------

select
  a.*,
  b.*,
  c.*,
  case
    When InsurerId = FromPartyId then Amount  * -1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid

where
  mainproduct = 'CI-MTB'
  and mainunit = 'LOC-DEU'
  and cashflowdate = 20190630
  and cashflowsubtype = 'ACQ'

-- COMMAND ----------

select distinct mainunit from db_202012_20220610_000047_502.contracts where MainPRoduct='CI-MTB'

-- COMMAND ----------

select distinct MainUnitMainProductSetId from sl1_20201231_v20220529_03.Expenses where MainUnitMainProductSetId like 'CI-MTB%'

-- COMMAND ----------

select
  a.*,
  b.*,
  c.*,
  case
    When InsurerId = FromPartyId then Amount  * -1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-DEU'
  and cashflowdate = 20181031
  and cashflowsubtype = 'MAI'

-- COMMAND ----------

with CTE as (select
  a.*,
  b.*,
  c.*,
  case
    When InsurerId = FromPartyId then Amount  * -1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-DEU'
  and cashflowdate = 20181031
  and cashflowsubtype = 'MAI') select sum(CustomValue) from CTE

-- COMMAND ----------

select * from MI2022.CashflowsExp_1_9 where productgroup = 'CI-ST'
  and mainunit = 'LOC-DEU'
  and cashflowdate = '2018-10-01'
  and cashflowsubtype = 'MAI' and ValuationDate='2020-12-31'

-- COMMAND ----------

select sum(amount) from sl1_20201231_v20220529_03.Expenses where MainUnitMainProductSetId='CI-ST_LOC-DEU' and cashflowdate=20181031 and cashflowsubtype='MAI'

-- COMMAND ----------

select
  a.*,
  b.*,
  c.*,
  d.*,
  Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount * ConversionRate * -1
    Else Amount * ConversionRate
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  left join db_202012_20220610_000047_502.Fxrates d on (
    TransactionCurrency = FromCurrency
    and ToCurrency = 'EUR'
    and CashflowDate = EffectToDate
  )
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-HUN'
  and cashflowdate = 20181031
  and cashflowsubtype = 'CHE'

-- COMMAND ----------

with CTE as (select
  a.*,
  b.*,
  c.*,
  d.*,
  Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount * ConversionRate * -1
    Else Amount * ConversionRate
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  left join db_202012_20220610_000047_502.Fxrates d on (
    TransactionCurrency = FromCurrency
    and ToCurrency = 'EUR'
    and CashflowDate = EffectToDate
  )
where
  mainproduct = 'CI-ST'
  and mainunit = 'LOC-HUN'
  and cashflowdate = 20181031
  and cashflowsubtype = 'CHE')
  
  select sum(CustomValue) from CTE

-- COMMAND ----------

select * from MI2022.CashflowsExp_1_9 where productgroup = 'CI-ST'
  and mainunit = 'LOC-HUN'
  and cashflowdate = '2018-10-01'
  and cashflowsubtype = 'CHE' and ValuationDate='2020-12-31'

-- COMMAND ----------

select a.*, ConversionRate,amount*ConversionRate from sl1_20201231_v20220529_03.Expenses a left join db_202012_20220610_000047_502.Fxrates d on (
    TransactionCurrency = FromCurrency
    and ToCurrency = 'EUR'
    and CashflowDate = EffectToDate
  ) where MainUnitMainProductSetId='CI-ST_LOC-HUN' and cashflowdate=20181031 and cashflowsubtype='CHE'

-- COMMAND ----------

select * from db_202012_20220610_000047_502.Fxrates where
    FromCurrency='HUF'
    and ToCurrency = 'EUR'
    and EffectToDate=20181031

-- COMMAND ----------

select * from db_202012_20220610_000047_502.Fxrates where ConversionRate=0.002780944965099141

-- COMMAND ----------

with CTE as (select
  a.*,
  b.*,
  c.*,
  case
    When InsurerId = FromPartyId then Amount  * -1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  
where
  mainproduct like 'SPU%'
  and mainunit = 'SPU-USA'
  and cashflowdate = 20200331
  and cashflowsubtype = 'ACQ')
  
  select *from CTE

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Expenses a where MainUnitMainProductSetId = 'SPU-ALL_SPU-USA' and cashflowdate=20200331 and cashflowsubtype='ACQ'

-- COMMAND ----------

with CTE as (select
  a.*,
  b.*,
  c.*,
  case
    When InsurerId = FromPartyId then Amount  * -1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  
where
  mainproduct='CI-ST'
  and mainunit = 'GLB-IRL'
  and cashflowdate = 20190430
  and cashflowsubtype = 'ACQ')
  
  select *from CTE

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Expenses a where MainUnitMainProductSetId = 'CI-ST_GLB-IRL' and cashflowdate=20190430 and cashflowsubtype='ACQ'

-- COMMAND ----------

select * from db_202012_20220610_000047_502.Datedistributions where Datedistributionid=-1289556442

-- COMMAND ----------

select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit from sl1_20201231_v20220529_03.Expenses a order by MainProduct asc

-- COMMAND ----------

select distinct MainProduct, MainUnit from db_202012_20220610_000047_502.contracts order by MainProduct asc

-- COMMAND ----------

select distinct datasource from db_202012_20220610_000047_502.contracts 

-- COMMAND ----------

-- MAGIC %md #NST

-- COMMAND ----------

-- MAGIC %md InsuredId is CustomerID

-- COMMAND ----------

select * from db_202012_20220610_000047_502.contracts where datasource='NST'

-- COMMAND ----------

select count(distinct insurerid) from db_202012_20220610_000047_502.contracts where datasource='NST'

-- COMMAND ----------

select * from db_202012_20220610_000047_502.entities --where InsurerId=3862573

-- COMMAND ----------

select count(distinct insuredid) from db_202012_20220610_000047_502.contracts where datasource='NST'

-- COMMAND ----------

-- MAGIC %md #BEY

-- COMMAND ----------

-- MAGIC %md InsuredId is CustomerID

-- COMMAND ----------

select count(*) from db_202012_20220610_000047_502.contracts where datasource='BEY'

-- COMMAND ----------

select * from db_202012_20220610_000047_502.contracts where datasource='BEY' and policyid='AR0606073'

-- COMMAND ----------

select count(distinct insuredid) from db_202012_20220610_000047_502.contracts where datasource='BEY'

-- COMMAND ----------

with CTE as (select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,a.* from sl1_20201231_v20220529_03.Expenses  a  where cashflowsubtype in ('MAI','ACQ','CHE'))

select * from  CTE where MainProduct like 'BO%' and mainunit='BON-ESP' and CashflowDate=20201231 

-- COMMAND ----------

-- WITH FxRates as (
--         select  ValuationDate,EffectFromDate,EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
--         from   db_202012_20220610_000047_502.fxrates
--         union all 
--         select * from db_202012_20220610_000047_502.fxrates
--         )
select
  a.*,
  b.*,
  c.*,
 -- d.*,
  --Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount *-1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
--   left join Fxrates d on (
--     TransactionCurrency = FromCurrency
--     and ToCurrency = 'EUR'
--     and CashflowDate = EffectToDate
--   )
where
  mainproduct like 'BO%'
  and CashflowDate=20201231
  and mainunit = 'BON-ESP'
  and cashflowsubtype = 'ACQ'


-- COMMAND ----------

with CTE as (select
  a.*,
  b.*,
  c.*,
 -- d.*,
  --Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount *-1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
--   left join Fxrates d on (
--     TransactionCurrency = FromCurrency
--     and ToCurrency = 'EUR'
--     and CashflowDate = EffectToDate
--   )
where
  mainproduct like 'BO%'
  and mainunit = 'BON-ESP'
  and CashflowDate=20201231
  and cashflowsubtype = 'ACQ')
  
  select sum(CustomValue) from CTE

-- COMMAND ----------


select
  distinct transactioncurrency
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
  
where
  mainproduct= 'CI-ST'
  and mainunit = 'GLB-CHN'
  and cashflowdate = 20201231
  and cashflowsubtype = 'CHE'


-- COMMAND ----------

-- WITH FxRates as (
--         select  ValuationDate,EffectFromDate,EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
--         from   db_202012_20220610_000047_502.fxrates
--         union all 
--         select * from db_202012_20220610_000047_502.fxrates
--         )
select
  a.*,
  b.*,
  c.*,
 -- d.*,
  --Amount * ConversionRate as AmountEur,
  case
    When InsurerId = FromPartyId then Amount *-1
    Else Amount 
  End as CustomValue
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid
--   left join Fxrates d on (
--     TransactionCurrency = FromCurrency
--     and ToCurrency = 'EUR'
--     and CashflowDate = EffectToDate
--   )
where

   mainunit = 'BON-NOR'
   and c.Cashflowdate<=c.ValuationDate


-- COMMAND ----------

With CTE as (select
  distinct MainUnit,MainProduct,CashflowSubtype,CashflowDate
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid
where
   and c.Cashflowdate<=c.ValuationDate)
   


-- COMMAND ----------

select * from mi2022.ExpenseTable_1_9

-- COMMAND ----------

-- MAGIC %md Mapp between Contracts and Expenses

-- COMMAND ----------

use db_201912_20220610_000047_498;
select distinct
  mainunit,mainproduct,CashFlowSubType
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

Use SL1_20191231_v20220529_03;
select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,CashFlowSubType from Expenses where CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

use db_202003_20220610_000047_499;
select distinct
  mainunit,mainproduct,CashFlowSubType
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

Use SL1_20200331_v20220529_03;
select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,CashFlowSubType from Expenses where CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

use db_202006_20220610_000047_500;
select distinct
  mainunit,mainproduct,CashFlowSubType
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

Use SL1_20200630_v20220529_03;
select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,CashFlowSubType from Expenses where CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

use 202009_20220610_000047_501;
select distinct
  mainunit,mainproduct,CashFlowSubType
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

Use SL1_20200930_v20220529_03;
select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,CashFlowSubType from Expenses where CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

use 202012_20220610_000047_502;
select distinct
  mainunit,mainproduct,CashFlowSubType
from
  contracts a
  left join cashflows b on a.contractid = b.contractid
  left join Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------

Use SL1_20201231_v20220529_03;
select distinct split(MainUnitMainProductSetId,'_')[0] as MainProduct, split(MainUnitMainProductSetId,'_')[1] as MainUnit,CashFlowSubType from Expenses where CashFlowSubType in ('MAI','CHE','ACQ')

-- COMMAND ----------



-- COMMAND ----------

with CTE as (
Select distinct
  a.ValuationDate,mainunit,mainproduct,CashFlowSubType
from
  db_201912_20220610_000047_498.contracts a
  left join db_201912_20220610_000047_498.cashflows b on a.contractid = b.contractid
  left join db_201912_20220610_000047_498.Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')
union all
Select distinct
    a.ValuationDate,mainunit,mainproduct,CashFlowSubType
from
  db_202003_20220610_000047_499.contracts a
  left join db_202003_20220610_000047_499.cashflows b on a.contractid = b.contractid
  left join db_202003_20220610_000047_499.Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')
  union all
Select distinct
    a.ValuationDate,mainunit,mainproduct,CashFlowSubType
from
  db_202006_20220610_000047_500.contracts a
  left join db_202006_20220610_000047_500.cashflows b on a.contractid = b.contractid
  left join db_202006_20220610_000047_500.Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')
  union all
Select distinct
    a.ValuationDate,mainunit,mainproduct,CashFlowSubType
from
  db_202009_20220610_000047_501.contracts a
  left join db_202009_20220610_000047_501.cashflows b on a.contractid = b.contractid
  left join db_202009_20220610_000047_501.Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ')
    union all
Select distinct
    a.ValuationDate,mainunit,mainproduct,CashFlowSubType
from
  db_202012_20220610_000047_502.contracts a
  left join db_202012_20220610_000047_502.cashflows b on a.contractid = b.contractid
  left join db_202012_20220610_000047_502.Datedistributions c on b.Datedistributionid = c.Datedistributionid where c.CashflowDate<=c.ValuationDate and CashFlowSubType in ('MAI','CHE','ACQ'))
  
select distinct mainunit,mainproduct,CashFlowSubType from CTE
