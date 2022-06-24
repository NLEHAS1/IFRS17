-- Databricks notebook source
-- MAGIC %md #Expense Table

-- COMMAND ----------

drop view  mi2022.ExpenseTable_1_9_Agg 

-- COMMAND ----------

Create view mi2022.ExpenseTable_1_9_Agg as
select ProductGroup,
UnitGroup,
CashflowDate,
ValuationDate,
MainUnit,
CashFlowType,
CashFlowSubType,
sum(AmountEUR) as TotalEuro
from mi2022.ExpenseTable_1_9 
group by 
ProductGroup,
UnitGroup,
CashflowDate,
ValuationDate,
MainUnit,
CashFlowType,
CashFlowSubType

-- COMMAND ----------

select * from mi2022.ExpenseTable_1_9_Agg

-- COMMAND ----------

select count(*) from mi2022.ExpenseTable_1_9_Agg

-- COMMAND ----------

-- MAGIC %md #CashflowsSubTypeMetrics_1_9 

-- COMMAND ----------

desc mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

drop view MI2022.CashflowsExp_1_9

-- COMMAND ----------

Create view MI2022.CashflowsExp_1_9 as
select
ValuationDate,
MainUnit,
CashflowType,
CashFlowSubType,
case
When MainProduct like 'CI%' then (split(MainProduct,'_')[0])
When MainProduct like 'RE%' then trim('-',left(MainProduct,charindex('-', MainProduct,4)))
else (split(MainProduct,'-')[0])
end as ProductGroup,
CAshflowMonth,
UnitGroup,
sum(CustomValue) as TotalEuro
from mireporting.CashflowsSubTypeMetrics_1_9
where CashflowNature='Actual Cashflow' and CashFlowSubType in ('MAI', 'ACQ', 'CHE') and mainproduct is not null
group by 
ValuationDate,
MainUnit,
CashflowType,
CashFlowSubType,
ProductGroup,
CAshflowMonth,
UnitGroup,
MainProduct

-- COMMAND ----------

select count(*) from MI2022.CashflowsExp_1_9

-- COMMAND ----------

select * from MI2022.CashflowsExp_1_9  where mainunit='BON-NOR'

-- COMMAND ----------

select * from mireporting.CashflowsSubTypeMetrics_1_9 where mainunit='BON-NOR'

-- COMMAND ----------

select distinct valuationdate from mi2022.ExpenseTable_1_9_Agg 

-- COMMAND ----------

With CTE as (
select
ValuationDate,
MainUnit,
CashflowType,
CashFlowSubType,
case
When MainProduct like 'CI%' then (split(MainProduct,'_')[0])
When MainProduct like 'RE%' then trim('-',left(MainProduct,charindex('-', MainProduct,4)))
else (split(MainProduct,'-')[0])
end as ProductGroup,
CustomCashflowDate as CashflowDate,
UnitGroup,
sum(CustomValue) as TotalEuro
from mireporting.CashflowsSubTypeMetrics_1_9
where CashflowNature='Actual Cashflow' and CashFlowSubType in ('MAI', 'ACQ', 'CHE') and mainproduct is not null
group by 
ValuationDate,
MainUnit,
CashflowType,
CashFlowSubType,
ProductGroup,
CashflowDate,
UnitGroup,
MainProduct)

select * from CTE where ValuationDate='2020-12-31' and MainUnit='LOC-DEU' and Cashflowsubtype='ACQ' and ProductGroup='CI-MTB' and cashflowdate='2019-06-01'
