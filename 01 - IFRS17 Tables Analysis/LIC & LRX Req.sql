-- Databricks notebook source
-- MAGIC %md To claculate if it LIC or LRC use LossEventDate: 
-- MAGIC 
-- MAGIC If LossEventDate > ValuationDate then it's LRC
-- MAGIC 
-- MAGIC If LossEventDate <= ValuationDate then it's LIC

-- COMMAND ----------

select a.*, 
Case
When LossEventDate <= ValuationDate then 'LIC'
Else 'LRC'
End as L
from MI2022.datedistributions_202003_20220412_081208_191 a

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
      MI2022.datedistributions_202012_20220528_010617_253
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
      Case
      When LossEventDate <= ValuationDate then 'LIC'
      Else 'LRC'
      End as Claims,
      sum(weight) as weight
    from
      Datedistributionsint
    group by
      DateDistributionid,
      ValuationDate,
      CustomCashflowDate,
      Claims
  )
  select
    DateDistributionid,
    ValuationDate,
    CustomCashflowDate,
    Claims,
    cast(round(weight) as int) as Weight -- Cast the weight as integer will solve the issue of summing the weights
  from
    Datedistributionsint2
)

select a.*,
CASE
    when CustomCashflowDate > ValuationDate then "Future Cashflow"
    Else "Actual Cashflow"
  End as CashflowNature
from DateDistributionsTrans a where datedistributionid=-2142832181

-- COMMAND ----------

select * from MI2022.datedistributions_202012_20220528_010617_253 where LossEventDate>ValuationDate 

-- COMMAND ----------

select count(*) from (
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
      MI2022.datedistributions_202012_20220528_010617_253
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
      Case
      When LossEventDate <= ValuationDate then 'LIC'
      Else 'LRC'
      End as Claims,
      sum(weight) as weight
    from
      Datedistributionsint
    group by
      DateDistributionid,
      ValuationDate,
      CustomCashflowDate,
      Claims
  )
  select
    DateDistributionid,
    ValuationDate,
    CustomCashflowDate,
    Claims,
    cast(round(weight) as int) as Weight -- Cast the weight as integer will solve the issue of summing the weights
  from
    Datedistributionsint2
)

select a.*,
CASE
    when CustomCashflowDate > ValuationDate then "Future Cashflow"
    Else "Actual Cashflow"
  End as CashflowNature
from DateDistributionsTrans a )

-- COMMAND ----------

select count(*) from (
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
      MI2022.datedistributions_202012_20220528_010617_253
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

select a.*,
CASE
    when CustomCashflowDate > ValuationDate then "Future Cashflow"
    Else "Actual Cashflow"
  End as CashflowNature
from DateDistributionsTrans a )

-- COMMAND ----------


with FXRATESEXPANDED (
select
ValuationDate, EffectFromDate, EffectToDate, FromCurrency, ToCurrency, ConversionRate, ConversionType
from MI2022.fxrates_202009_20220528_010617_252
union
        select distinct ValuationDate, EffectFromDate, EffectToDate, FromCurrency, FromCurrency as ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
 union
        select distinct ValuationDate, EffectFromDate, EffectToDate, ToCurrency as FromCurrency, ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
)
select

FromCurrency, ToCurrency, ConversionType, DateDistributionId,

                sum(Weight) as Weight,

                sum(Weight*ConversionRate) as WeightEuros, 

                case when invoiceDate <= 20191231 then "Past" else

                (case when (invoiceDate > 20191231 and invoiceDate <= 20201231) then "InPeriod" else "Future" end) end as InvoiceOccurence,

                case when CashflowDate <= 20191231 then "Past" else

                (case when (CashflowDate > 20191231 and CashflowDate <= 20201231) then "InPeriod" else "Future" end) end as CashflowOccurence,

                case when LossEventDate <= 20191231 then "Past" else

                (case when (LossEventDate > 20191231 and LossEventDate <= 20201231) then "InPeriod" else "Future" end) end as LossEventOccurence,

                case when ReceivableDueDate <= 20191231 then "Past" else

                (case when (ReceivableDueDate > 20191231 and ReceivableDueDate <= 20201231) then "InPeriod" else "Future" end) end as ReceivableDueOccurence,

                "Unavailable" as ClaimReceivedOccurence

               from MI2022.datedistributions_202012_20220528_010617_253 a, FXRATESEXPANDED b

               where least(a.CashflowDate, a.valuationDate) = b.EffectToDate and ToCurrency = "EUR" and ConversionType = "SPOT" --very nice code to extract the right conversionrate for actuals and futures

               group by DateDistributionId, FromCurrency, ToCurrency, ConversionType, InvoiceOccurence, CashflowOccurence, LossEventOccurence, ClaimReceivedOccurence, ReceivableDueOccurence;

-- COMMAND ----------

with Cte as (
with FXRATESEXPANDED (
select
ValuationDate, EffectFromDate, EffectToDate, FromCurrency, ToCurrency, ConversionRate, ConversionType
from MI2022.fxrates_202009_20220528_010617_252
union
        select distinct ValuationDate, EffectFromDate, EffectToDate, FromCurrency, FromCurrency as ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
 union
        select distinct ValuationDate, EffectFromDate, EffectToDate, ToCurrency as FromCurrency, ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
)
select

FromCurrency, ToCurrency, ConversionType, DateDistributionId,

                sum(Weight) as Weight,

                sum(Weight*ConversionRate) as WeightEuros, 

                case when invoiceDate <= 20191231 then "Past" else

                (case when (invoiceDate > 20191231 and invoiceDate <= 20201231) then "InPeriod" else "Future" end) end as InvoiceOccurence,

                case when CashflowDate <= 20191231 then "Past" else

                (case when (CashflowDate > 20191231 and CashflowDate <= 20201231) then "InPeriod" else "Future" end) end as CashflowOccurence,

                case when LossEventDate <= 20191231 then "Past" else

                (case when (LossEventDate > 20191231 and LossEventDate <= 20201231) then "InPeriod" else "Future" end) end as LossEventOccurence,

                case when ReceivableDueDate <= 20191231 then "Past" else

                (case when (ReceivableDueDate > 20191231 and ReceivableDueDate <= 20201231) then "InPeriod" else "Future" end) end as ReceivableDueOccurence,

                "Unavailable" as ClaimReceivedOccurence

               from MI2022.datedistributions_202012_20220528_010617_253 a, FXRATESEXPANDED b

               where least(a.CashflowDate, a.valuationDate) = b.EffectToDate and ToCurrency = "EUR" and ConversionType = "SPOT"

               group by DateDistributionId, FromCurrency, ToCurrency, ConversionType, InvoiceOccurence, CashflowOccurence, LossEventOccurence, ClaimReceivedOccurence, ReceivableDueOccurence)
             
select count(*) from Cte
 

-- COMMAND ----------

select count(*) from MI2022.datedistributions_202012_20220528_010617_253

-- COMMAND ----------

with Cte as (
with FXRATESEXPANDED (
select
ValuationDate, EffectFromDate, EffectToDate, FromCurrency, ToCurrency, ConversionRate, ConversionType
from MI2022.fxrates_202009_20220528_010617_252
union
        select distinct ValuationDate, EffectFromDate, EffectToDate, FromCurrency, FromCurrency as ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
 union
        select distinct ValuationDate, EffectFromDate, EffectToDate, ToCurrency as FromCurrency, ToCurrency, 1 as ConversionRate, ConversionType
        from MI2022.fxrates_202009_20220528_010617_252
        where FromCurrency = "EUR" or ToCurrency = "EUR"
)
select

FromCurrency, ToCurrency, ConversionType, DateDistributionId,

                sum(Weight) as Weight,

                sum(Weight*ConversionRate) as WeightEuros, 

                case when invoiceDate <= 20191231 then "Past" else

                (case when (invoiceDate > 20191231 and invoiceDate <= 20201231) then "InPeriod" else "Future" end) end as InvoiceOccurence,

                case when CashflowDate <= 20191231 then "Past" else

                (case when (CashflowDate > 20191231 and CashflowDate <= 20201231) then "InPeriod" else "Future" end) end as CashflowOccurence,

                case when LossEventDate <= 20191231 then "Past" else

                (case when (LossEventDate > 20191231 and LossEventDate <= 20201231) then "InPeriod" else "Future" end) end as LossEventOccurence,

                case when ReceivableDueDate <= 20191231 then "Past" else

                (case when (ReceivableDueDate > 20191231 and ReceivableDueDate <= 20201231) then "InPeriod" else "Future" end) end as ReceivableDueOccurence,

                "Unavailable" as ClaimReceivedOccurence

               from MI2022.datedistributions_202012_20220528_010617_253 a, FXRATESEXPANDED b

               where least(a.CashflowDate, a.valuationDate) = b.EffectToDate and ToCurrency = "EUR" and ConversionType = "SPOT"

               group by DateDistributionId, FromCurrency, ToCurrency, ConversionType, InvoiceOccurence, CashflowOccurence, LossEventOccurence, ClaimReceivedOccurence, ReceivableDueOccurence)
             
select * from CTE where weight!=1

 

-- COMMAND ----------

select * from db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd.datedistributions 
