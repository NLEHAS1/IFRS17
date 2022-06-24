-- Databricks notebook source
-- MAGIC %md #Cashflows Metrics 

-- COMMAND ----------

-- MAGIC %md ###1.201912_20220610_000047_498

-- COMMAND ----------

Use 201912_20220610_000047_498;

-- COMMAND ----------

Create or replace table mireporting.CashflowsMetrics_1_9 as 
select
  a.ValuationDate,
  a.CohortYear,
  a.RiskPeriodStartMonth,
  a.RiskPeriodStartYear,
  a.BoundDateMonth,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0) as Premium,
  COALESCE(a.Expenses, 0) as Expenses,
  COALESCE(a.Claims, 0) as Claims
from
  (
    select
      *
    from
      (
        WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from   fxrates
        union all 
        select * from fxrates
        ),
        
         DateDistributionsTrans as (
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
              datedistributions
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
        CTE0 as (
          SELECT
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            DateDistributionid,
            TransactionCurrency,
            sum(Amount) as Amount
          from
            cashflows where Datasource != 'RE2021'
          group by
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            TransactionCurrency,
            DateDistributionid
        ),
        CTE1 as(
          select
            a.ValuationDate,
            a.ContractId,
            a.MainUnit,
            a.MainProduct,
            a.InsurerId,
            Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
            Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
            Left(a.ContractIssueDate, 4) as CohortYear,
            Left(BoundDate, 6) as BoundDateMonth,
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
            contracts a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN Entities g on (a.InsurerId = g.InsurerId)
            LEFT JOIN Entities h on (a.InsuredId = h.InsurerId)
            LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
          where
            Left(a.ContractIssueDate, 6) >= 201501
            and a.Datasource != 'RE2021'
        ),
        CTE2 as (
          select
            a.ValuationDate,
            ContractId,
            MainUnit,
            MainProduct,
            InsurerId,
            to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
            to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
            to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
              ) then f.ConversionRate ---> Actual Cashflows
              Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
            End as ConversionRate,
            CASE
              When TransactionCurrency = 'EUR' then Amount
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then Amount * e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
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
              and CustomCashflowDate = f.EffectToDate
            ) ---> Actual Cashflows
        ),
        CTE3 as (
          --Only Atradius N.V
          select
            a.*,
            case
              When InsurerId = FromPartyId then AmountEUR * -1
              Else AmountEUR
            End as CustomValue
          from
            CTE2 a
        )
        select
          to_date(cast(ValuationDate as string), 'yyyyMMdd') as ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a

-- COMMAND ----------

select * from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select count(*) from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

-- MAGIC %md ###2.202003_20220610_000047_499

-- COMMAND ----------

Use 202003_20220610_000047_499;

-- COMMAND ----------

Insert into mireporting.CashflowsMetrics_1_9
select * from (
select
  a.ValuationDate,
  a.CohortYear,
  a.RiskPeriodStartMonth,
  a.RiskPeriodStartYear,
  a.BoundDateMonth,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0) as Premium,
  COALESCE(a.Expenses, 0) as Expenses,
  COALESCE(a.Claims, 0) as Claims
from
  (
    select
      *
    from
      (
        WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),
        
         DateDistributionsTrans as (
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
              datedistributions
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
        CTE0 as (
          SELECT
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            DateDistributionid,
            TransactionCurrency,
            sum(Amount) as Amount
          from
            cashflows where Datasource != 'RE2021'
          group by
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            TransactionCurrency,
            DateDistributionid
        ),
        CTE1 as(
          select
            a.ValuationDate,
            a.ContractId,
            a.MainUnit,
            a.MainProduct,
            a.InsurerId,
            Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
            Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
            Left(a.ContractIssueDate, 4) as CohortYear,
            Left(BoundDate, 6) as BoundDateMonth,
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
            contracts a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN Entities g on (a.InsurerId = g.InsurerId)
            LEFT JOIN Entities h on (a.InsuredId = h.InsurerId)
            LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
          where
            Left(a.ContractIssueDate, 6) >= 201501
            and a.Datasource != 'RE2021'
        ),
        CTE2 as (
          select
            a.ValuationDate,
            ContractId,
            MainUnit,
            MainProduct,
            InsurerId,
            to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
            to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
            to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
              ) then f.ConversionRate ---> Actual Cashflows
              Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
            End as ConversionRate,
            CASE
              When TransactionCurrency = 'EUR' then Amount
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then Amount * e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
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
              and CustomCashflowDate = f.EffectToDate
            ) ---> Actual Cashflows
        ),
        CTE3 as (
          --Only Atradius N.V
          select
            a.*,
            case
              When InsurerId = FromPartyId then AmountEUR * -1
              Else AmountEUR
            End as CustomValue
          from
            CTE2 a
        )
        select
          to_date(cast(ValuationDate as string), 'yyyyMMdd') as ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a)

-- COMMAND ----------

select count(*) from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

-- MAGIC %md ###3.202006_20220610_000047_500

-- COMMAND ----------

use 202006_20220610_000047_500;

-- COMMAND ----------

Insert into mireporting.CashflowsMetrics_1_9
select * from (
select
  a.ValuationDate,
  a.CohortYear,
  a.RiskPeriodStartMonth,
  a.RiskPeriodStartYear,
  a.BoundDateMonth,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0) as Premium,
  COALESCE(a.Expenses, 0) as Expenses,
  COALESCE(a.Claims, 0) as Claims
from
  (
    select
      *
    from
      (
        WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),
        
         DateDistributionsTrans as (
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
              datedistributions
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
        CTE0 as (
          SELECT
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            DateDistributionid,
            TransactionCurrency,
            sum(Amount) as Amount
          from
            cashflows where Datasource != 'RE2021'
          group by
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            TransactionCurrency,
            DateDistributionid
        ),
        CTE1 as(
          select
            a.ValuationDate,
            a.ContractId,
            a.MainUnit,
            a.MainProduct,
            a.InsurerId,
            Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
            Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
            Left(a.ContractIssueDate, 4) as CohortYear,
            Left(BoundDate, 6) as BoundDateMonth,
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
            contracts a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN Entities g on (a.InsurerId = g.InsurerId)
            LEFT JOIN Entities h on (a.InsuredId = h.InsurerId)
            LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
          where
            Left(a.ContractIssueDate, 6) >= 201501
            and a.Datasource != 'RE2021'
        ),
        CTE2 as (
          select
            a.ValuationDate,
            ContractId,
            MainUnit,
            MainProduct,
            InsurerId,
            to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
            to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
            to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
              ) then f.ConversionRate ---> Actual Cashflows
              Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
            End as ConversionRate,
            CASE
              When TransactionCurrency = 'EUR' then Amount
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then Amount * e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
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
              and CustomCashflowDate = f.EffectToDate
            ) ---> Actual Cashflows
        ),
        CTE3 as (
          --Only Atradius N.V
          select
            a.*,
            case
              When InsurerId = FromPartyId then AmountEUR * -1
              Else AmountEUR
            End as CustomValue
          from
            CTE2 a
        )
        select
          to_date(cast(ValuationDate as string), 'yyyyMMdd') as ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a)

-- COMMAND ----------

select count(*) from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

-- MAGIC %md ###4.202009_20220610_000047_501

-- COMMAND ----------

Use 202009_20220610_000047_501;

-- COMMAND ----------

Insert into mireporting.CashflowsMetrics_1_9
select * from (
select
  a.ValuationDate,
  a.CohortYear,
  a.RiskPeriodStartMonth,
  a.RiskPeriodStartYear,
  a.BoundDateMonth,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0) as Premium,
  COALESCE(a.Expenses, 0) as Expenses,
  COALESCE(a.Claims, 0) as Claims
from
  (
    select
      *
    from
      (
        WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),
        
         DateDistributionsTrans as (
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
              datedistributions
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
        CTE0 as (
          SELECT
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            DateDistributionid,
            TransactionCurrency,
            sum(Amount) as Amount
          from
            cashflows where Datasource != 'RE2021'
          group by
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            TransactionCurrency,
            DateDistributionid
        ),
        CTE1 as(
          select
            a.ValuationDate,
            a.ContractId,
            a.MainUnit,
            a.MainProduct,
            a.InsurerId,
            Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
            Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
            Left(a.ContractIssueDate, 4) as CohortYear,
            Left(BoundDate, 6) as BoundDateMonth,
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
            contracts a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN Entities g on (a.InsurerId = g.InsurerId)
            LEFT JOIN Entities h on (a.InsuredId = h.InsurerId)
            LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
          where
            Left(a.ContractIssueDate, 6) >= 201501
            and a.Datasource != 'RE2021'
        ),
        CTE2 as (
          select
            a.ValuationDate,
            ContractId,
            MainUnit,
            MainProduct,
            InsurerId,
            to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
            to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
            to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
              ) then f.ConversionRate ---> Actual Cashflows
              Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
            End as ConversionRate,
            CASE
              When TransactionCurrency = 'EUR' then Amount
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then Amount * e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
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
              and CustomCashflowDate = f.EffectToDate
            ) ---> Actual Cashflows
        ),
        CTE3 as (
          --Only Atradius N.V
          select
            a.*,
            case
              When InsurerId = FromPartyId then AmountEUR * -1
              Else AmountEUR
            End as CustomValue
          from
            CTE2 a
        )
        select
          to_date(cast(ValuationDate as string), 'yyyyMMdd') as ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a)

-- COMMAND ----------

select count(*) from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

-- MAGIC %md ###5.202012_20220610_000047_502

-- COMMAND ----------

Use 202012_20220610_000047_502;

-- COMMAND ----------

Insert into mireporting.CashflowsMetrics_1_9
select * from (
select
  a.ValuationDate,
  a.CohortYear,
  a.RiskPeriodStartMonth,
  a.RiskPeriodStartYear,
  a.BoundDateMonth,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0) as Premium,
  COALESCE(a.Expenses, 0) as Expenses,
  COALESCE(a.Claims, 0) as Claims
from
  (
    select
      *
    from
      (
        WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),
        
         DateDistributionsTrans as (
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
              datedistributions
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
        CTE0 as (
          SELECT
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            DateDistributionid,
            TransactionCurrency,
            sum(Amount) as Amount
          from
            cashflows where Datasource != 'RE2021'
          group by
            ContractId,
            CashFlowId,
            CashFlowType,
            CashFlowSubType,
            FromPartyId,
            ToPartyId,
            TransactionCurrency,
            DateDistributionid
        ),
        CTE1 as(
          select
            a.ValuationDate,
            a.ContractId,
            a.MainUnit,
            a.MainProduct,
            a.InsurerId,
            Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
            Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
            Left(a.ContractIssueDate, 4) as CohortYear,
            Left(BoundDate, 6) as BoundDateMonth,
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
            contracts a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN Entities g on (a.InsurerId = g.InsurerId)
            LEFT JOIN Entities h on (a.InsuredId = h.InsurerId)
            LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
          where
            Left(a.ContractIssueDate, 6) >= 201501
            and a.Datasource != 'RE2021'
        ),
        CTE2 as (
          select
            a.ValuationDate,
            ContractId,
            MainUnit,
            MainProduct,
            InsurerId,
            to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
            to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
            to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
              ) then f.ConversionRate ---> Actual Cashflows
              Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
            End as ConversionRate,
            CASE
              When TransactionCurrency = 'EUR' then Amount
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate > e.ValuationDate
              ) then Amount * e.ConversionRate ---> Future Cashflows
              When (
                TransactionCurrency != 'EUR'
                and CustomCashflowDate <= f.ValuationDate
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
              and CustomCashflowDate = f.EffectToDate
            ) ---> Actual Cashflows
        ),
        CTE3 as (
          --Only Atradius N.V
          select
            a.*,
            case
              When InsurerId = FromPartyId then AmountEUR * -1
              Else AmountEUR
            End as CustomValue
          from
            CTE2 a
        )
        select
          to_date(cast(ValuationDate as string), 'yyyyMMdd') as ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          RiskPeriodStartMonth,
          RiskPeriodStartYear,
          CohortYear,
          BoundDateMonth,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a)

-- COMMAND ----------

select count(*) from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsMetrics_1_9 

-- COMMAND ----------

-- MAGIC %md #CashflowsSubTypes Metrics

-- COMMAND ----------

-- MAGIC %md ###1.201912_20220610_000047_498

-- COMMAND ----------

Use 201912_20220610_000047_498;

-- COMMAND ----------

Create or replace table mireporting.CashflowsSubTypeMetrics_1_9 as 
WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),

 DateDistributionsTrans as (
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
      datedistributions
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  cashflows
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
  Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(BoundDate, 6) as BoundDateMonth,
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
    contracts a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN Entities g on (a.InsurerId = g.InsurerId )
  LEFT JOIN Entities h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
  to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
    LEFT JOIN FxRates e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN FxRates f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

  select
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.RiskPeriodStartMonth,
    a.RiskPeriodStartYear,
    a.CohortYear,
    a.BoundDateMonth,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    Sum(a.CustomValue) as CustomValue
  from
    CTE3 a
  group by
    ValuationDate,
    RiskPeriodStartMonth,
    RiskPeriodStartYear,
    CohortYear,
    BoundDateMonth,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct

-- COMMAND ----------

select * from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select count(*) from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

-- MAGIC %md ###2.202003_20220610_000047_499

-- COMMAND ----------

Use 202003_20220610_000047_499;

-- COMMAND ----------

Insert into mireporting.CashflowsSubTypeMetrics_1_9
select * from (
WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),

 DateDistributionsTrans as (
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
      datedistributions
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  cashflows
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
  Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(BoundDate, 6) as BoundDateMonth,
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
    contracts a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN Entities g on (a.InsurerId = g.InsurerId )
  LEFT JOIN Entities h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
  to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
    LEFT JOIN FxRates e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN FxRates f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

  select
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.RiskPeriodStartMonth,
    a.RiskPeriodStartYear,
    a.CohortYear,
    a.BoundDateMonth,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    Sum(a.CustomValue) as CustomValue
  from
    CTE3 a
  group by
    ValuationDate,
    RiskPeriodStartMonth,
    RiskPeriodStartYear,
    CohortYear,
    BoundDateMonth,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct)

-- COMMAND ----------

select count(*) from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

-- MAGIC %md ###3.202006_20220610_000047_500

-- COMMAND ----------

use 202006_20220610_000047_500;

-- COMMAND ----------

Insert into mireporting.CashflowsSubTypeMetrics_1_9
select * from (
WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),

 DateDistributionsTrans as (
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
      datedistributions
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  cashflows
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
  Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(BoundDate, 6) as BoundDateMonth,
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
    contracts a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN Entities g on (a.InsurerId = g.InsurerId )
  LEFT JOIN Entities h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
  to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
    LEFT JOIN FxRates e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN FxRates f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

  select
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.RiskPeriodStartMonth,
    a.RiskPeriodStartYear,
    a.CohortYear,
    a.BoundDateMonth,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    Sum(a.CustomValue) as CustomValue
  from
    CTE3 a
  group by
    ValuationDate,
    RiskPeriodStartMonth,
    RiskPeriodStartYear,
    CohortYear,
    BoundDateMonth,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct)

-- COMMAND ----------

select count(*) from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

-- MAGIC %md ###4.202009_20220610_000047_501

-- COMMAND ----------

Use 202009_20220610_000047_501;

-- COMMAND ----------

Insert into mireporting.CashflowsSubTypeMetrics_1_9
select * from (
WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),

 DateDistributionsTrans as (
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
      datedistributions
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  cashflows
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
  Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(BoundDate, 6) as BoundDateMonth,
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
    contracts a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN Entities g on (a.InsurerId = g.InsurerId )
  LEFT JOIN Entities h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
  to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
    LEFT JOIN FxRates e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN FxRates f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

  select
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.RiskPeriodStartMonth,
    a.RiskPeriodStartYear,
    a.CohortYear,
    a.BoundDateMonth,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    Sum(a.CustomValue) as CustomValue
  from
    CTE3 a
  group by
    ValuationDate,
    RiskPeriodStartMonth,
    RiskPeriodStartYear,
    CohortYear,
    BoundDateMonth,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct)

-- COMMAND ----------

select count(*) from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

-- MAGIC %md ###5.202012_20220610_000047_502

-- COMMAND ----------

Use 202012_20220610_000047_502;

-- COMMAND ----------

Insert into mireporting.CashflowsSubTypeMetrics_1_9
select * from (
WITH FxRates as (
        select distinct ValuationDate,ValuationDate as EffectFromDate,ValuationDate as EffectToDate,CalculationEntity,'EUR' as FromCurrency, 'EUR' as  ToCurrency, 1 as ConversionRate,ConversionType,DataSource,Context 
        from    fxrates
        union all 
        select * from fxrates
        ),

 DateDistributionsTrans as (
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
      datedistributions
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

CTE0 as (
  SELECT
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  DateDistributionid,
  TransactionCurrency,
  sum(Amount) as Amount
  from
  cashflows
  group by
  ContractId,
  CashFlowId,
  CashFlowType,
  CashFlowSubType,
  FromPartyId,
  ToPartyId,
  TransactionCurrency,
  DateDistributionid
),

CTE1 as(
  select
  a.ValuationDate,
  a.ContractId,
  a.MainUnit,
  a.MainProduct,
  a.InsurerId,
  Left(RiskPeriodStartDate, 6) as RiskPeriodStartMonth,
  Left(RiskPeriodStartDate, 4) as RiskPeriodStartYear,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(BoundDate, 6) as BoundDateMonth,
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
    contracts a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN Entities g on (a.InsurerId = g.InsurerId )
  LEFT JOIN Entities h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN Entities i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN Entities j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(RiskPeriodStartMonth as string), 'yyyyMM') as RiskPeriodStartMonth,
  to_date(cast(RiskPeriodStartYear as string), 'y') as RiskPeriodStartYear,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
  to_date(cast(BoundDateMonth as string), 'yyyyMM') as BoundDateMonth,
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
    LEFT JOIN FxRates e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN FxRates f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
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

  select
    to_date(cast(a.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
    a.RiskPeriodStartMonth,
    a.RiskPeriodStartYear,
    a.CohortYear,
    a.BoundDateMonth,
    a.MainUnit,
    a.CashflowNature,
    a.CashflowType,
    a.CashFlowSubType,
    a.MainProduct,
    Sum(a.CustomValue) as CustomValue
  from
    CTE3 a
  group by
    ValuationDate,
    RiskPeriodStartMonth,
    RiskPeriodStartYear,
    CohortYear,
    BoundDateMonth,
    MainUnit,
    CashflowNature,
    CashflowType,
    CashFlowSubType,
    MainProduct)

-- COMMAND ----------

select count(*) from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select distinct valuationdate from mireporting.CashflowsSubTypeMetrics_1_9

-- COMMAND ----------

select * from  mireporting.CashflowsSubTypeMetrics_1_9
