-- Databricks notebook source
create table default.Expenses20191231v20220215_01
using orc
options (path="/mnt/sl1/DATA/SL1/20191231.v20220215_01/SourceData/Expenses/Tagetik")

-- COMMAND ----------

desc extended default.Expenses20191231v20220215_01

-- COMMAND ----------

select * from default.Expenses20191231v20220215_01

-- COMMAND ----------

select MainProduct, MainUnit,CashFlowSubType,sum(amount) from default.Expenses20191231v20220215_01 group by MainProduct, MainUnit,CashFlowSubType  

-- COMMAND ----------

-- MAGIC %py 
-- MAGIC ExpensesPy=sqlContext.sql('select * from default.Expenses20191231v20220215_01')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC Expensesdf=ExpensesPy.summary()
-- MAGIC display(Expensesdf)

-- COMMAND ----------

select count(*) from default.Expenses20191231v20220215_01

-- COMMAND ----------

select distinct datasource from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md #MainProduct

-- COMMAND ----------

-- MAGIC %md The following code show us the MainProducts in Expenses table

-- COMMAND ----------

select distinct mainproduct from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md The following code show us the MainProducts that are available in Expenses but not in Contracts

-- COMMAND ----------

select distinct mainproduct from default.Expenses20191231v20220215_01 a left anti join  db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.contracts b on a.mainProduct=b.mainProduct

-- COMMAND ----------

-- MAGIC %md The following code show us the common MainProducts between Contracts and Expenses

-- COMMAND ----------

select distinct a.mainproduct from default.Expenses20191231v20220215_01 a inner join  db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.contracts b on a.mainProduct=b.mainProduct

-- COMMAND ----------

-- MAGIC %md #MainUnit

-- COMMAND ----------

select distinct mainUnit from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md The following code show us the common and variance MainUnits between Contracts and Expenses 

-- COMMAND ----------

select distinct a.mainUnit,b.mainunit from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.contracts a  full outer join  default.Expenses20191231v20220215_01 b on a.mainUnit=b.mainUnit

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.contracts where mainunit='LOC-BRA'

-- COMMAND ----------

-- MAGIC %md #ProductType

-- COMMAND ----------

select distinct ProductType from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md #Unit

-- COMMAND ----------

select distinct unit from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md #CashflowType

-- COMMAND ----------

select distinct CashflowType from default.Expenses20191231v20220215_01

-- COMMAND ----------

-- MAGIC %md #Cashflowsubtype

-- COMMAND ----------

select distinct Cashflowsubtype from default.Expenses20191231v20220215_01 

-- COMMAND ----------

select distinct cashflowsubtype from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.cashflows where cashflowtype='E'

-- COMMAND ----------

-- MAGIC %md #ModelId

-- COMMAND ----------

-- MAGIC %md As we can see there's no overlap between ModelId in Cashflows and Expenses

-- COMMAND ----------

select distinct ModelId from default.Expenses20191231v20220215_01 

-- COMMAND ----------

select distinct ModelId from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.Cashflows

-- COMMAND ----------

-- MAGIC %md #TransactionCurrency

-- COMMAND ----------

select distinct TransactionCurrency from default.Expenses20191231v20220215_01 

-- COMMAND ----------

-- MAGIC %md #Joining Cashflows to Expenses

-- COMMAND ----------

select * from default.Expenses20191231v20220215_01 order by CashFlowDate desc

-- COMMAND ----------

select * from db_201912_20220222_091937_113.Cashflows where datasource='SYM' --contractid='SYM / 516125 / 20180701' and cashflowid='3264611285181946915'

-- COMMAND ----------

select * from db_201912_20220222_091937_113.Cashflows where contractid='SYM / 516125 / 20180701' and cashflowid='3264611288403281188'

-- COMMAND ----------

select * from db_201912_20220222_091937_113.datedistributions where datedistributionid='-2098267990'

-- COMMAND ----------

select
  *
from
  db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.Cashflows a
  left join db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.Contracts b on a.ContractId = b.ContractId
  left join MI2022.Expenses_20191231_v20220228_01 c on (
    b.MainProduct = c.MainProduct
    and b.MainUnit = c.MainUnit
    and a.CashflowType = c.CashflowType
    and a.Cashflowsubtype = c.Cashflowsubtype
    and a.frompartyid = c.frompartyid
    and a.topartyid = c.topartyid
  )
where b.DataSource='SYM'

-- COMMAND ----------

-- MAGIC %md as you can see the amounts don't add up

-- COMMAND ----------

select
  *
from
  db_201912_20220222_091937_113.Cashflows a
  left join db_201912_20220222_091937_113.DateDistributions b on a.DateDistributionId=b.DateDistributionId
where
  FromPartyId = 17681259
  and ToPartyId = 'External'
  and TransactionCurrency = 'EUR'
  and CashFlowSubType = 'CHE'
  and CashFlowType = 'E'
  and modelid = 'ClaimsExpensesActuals_1838fdb2abd3cd89416101f58e790d96'
  and cashflowDate=20191231

-- COMMAND ----------


