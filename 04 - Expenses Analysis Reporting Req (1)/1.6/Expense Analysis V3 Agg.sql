-- Databricks notebook source
Create or replace table mireporting.expenseanalysis_1_6_Agg as 
select
  a.ValuationDate,
  a.CohortMonth,
  a.CohortYear,
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
              MI2022.datedistributions_201912_20220528_010617_249
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
            MI2022.cashflows2_201912_20220528_010617_249
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
            Left(a.ContractIssueDate, 6) as CohortMonth,
            Left(a.ContractIssueDate, 4) as CohortYear,
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
            MI2022.contracts_201912_20220528_010617_249 a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 g on (a.InsurerId = g.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 h on (a.InsuredId = h.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 j on (c.ToPartyId = j.InsurerId)
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
            to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
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
            LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 e on (
              TransactionCurrency = e.FromCurrency
              and e.ToCurrency = 'EUR'
              and e.ValuationDate = e.EffectToDate
            ) ---> Future Cashflows
            LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 f on (
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
          CohortMonth,
          CohortYear,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          CohortMonth,
          CohortYear,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a

-- COMMAND ----------

select count(*) from mireporting.expenseanalysis_1_6_Agg

-- COMMAND ----------

select * from mireporting.expenseanalysis_1_6_Agg                                                                                                    where mainunit='BON-ESP' and cohortyear='2015-01-01'

-- COMMAND ----------

select ValuationDate, CohortMonth, CohortYear,MainUnit, CashflowNature, MainProduct, count(*) from mireporting.expenseanalysis_1_6_Agg  group by 
ValuationDate, CohortMonth, CohortYear,MainUnit, CashflowNature, MainProduct having count(*)>1

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
      MI2022.datedistributions_201912_20220528_010617_249
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
  MI2022.cashflows2_201912_20220528_010617_249
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
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_201912_20220528_010617_249 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
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
    LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
),

CTE3 as ( --Only Atradius N.V
  select
    a.*,
    case
      When InsurerId = FromPartyId then AmountEUR * -1
      Else AmountEUR
    End as CustomValue
  from
    CTE2 a where mainunit='BON-ESP' and cohortyear='2015-01-01'
),

CTEP as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Premium
  from
    CTE3
  where
    Cashflowtype = 'P'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEE as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Expenses
  from
    CTE3
  where
    Cashflowtype = 'E'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
),

CTEC as (
  select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    Sum(CustomValue) as Claims
  from
    CTE3
  where
    Cashflowtype = 'C'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct
)

select
  p.CohortMonth,
  p.CohortYear,
  p.CashflowNature,
  to_date(cast(p.ValuationDate as string), 'yyyyMMdd') as ValuationDate,
  p.MainUnit,
  p.MainProduct,
  COALESCE(Premium,0) as Premium,
  COALESCE(Expenses,0) as Expenses,
  COALESCE(Claims,0) as Claims
from
  CTEP p
  left join CTEE e on (
    p.CohortMonth = e.CohortMonth
    and p.MainUnit=e.MainUnit and p.MainProduct=e.MainProduct and p.ValuationDate=e.ValuationDate and p.CashflowNature=e.CashflowNature
  )
  left join CTEC c on (
    p.CohortMonth = c.CohortMonth
    and p.MainUnit=c.MainUnit and p.MainProduct=c.MainProduct and p.ValuationDate=c.ValuationDate and p.CashflowNature=c.CashflowNature
  )

-- COMMAND ----------

select * from (
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
      MI2022.datedistributions_201912_20220528_010617_249
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
  MI2022.cashflows2_201912_20220528_010617_249
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
  Left(a.ContractIssueDate, 6) as CohortMonth,
  Left(a.ContractIssueDate, 4) as CohortYear,
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
    MI2022.contracts_201912_20220528_010617_249 a
  LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
  LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 g on (a.InsurerId = g.InsurerId )
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 h on ( a.InsuredId = h.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 i on (c.FromPartyId = i.InsurerId)
  LEFT JOIN MI2022.Entities_201912_20220528_010617_249 j on (c.ToPartyId = j.InsurerId)
  where Left(a.ContractIssueDate, 6) >= 201501 and a.Datasource!='RE2021' 
),

CTE2 as (
select a.ValuationDate,
  ContractId,
  MainUnit,
  MainProduct,
  InsurerId,
  to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
  to_date(cast(CohortYear as string), 'y') as CohortYear,
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
    LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 e on (TransactionCurrency = e.FromCurrency and e.ToCurrency = 'EUR' and e.ValuationDate = e.EffectToDate) ---> Future Cashflows
    LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 f on (TransactionCurrency = f.FromCurrency and f.ToCurrency = 'EUR'and CustomCashflowDate = f.EffectToDate ) ---> Actual Cashflows 
),

  CTE3 as ( --Only Atradius N.V
  select
    a.*,
    case
      When InsurerId = FromPartyId then AmountEUR * -1
      Else AmountEUR
    End as CustomValue
  from
    CTE2 a where mainunit='BON-ESP' and cohortyear='2015-01-01'
)

select
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    MainProduct,
    CashflowType,
    Sum(CustomValue) as CustomValue
  from
    CTE3
  where mainunit='BON-ESP' and cohortyear='2015-01-01'
  group by
    ValuationDate,
    CohortMonth,
    CohortYear,
    MainUnit,
    CashflowNature,
    CashflowType,
    MainProduct
 )
  pivot (MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims))

-- COMMAND ----------

select
  a.ValuationDate,
  a.CohortMonth,
  a.CohortYear,
  a.MainUnit,
  a.CashflowNature,
  a.MainProduct,
  COALESCE(a.Premium, 0),
  COALESCE(a.Expenses, 0),
  COALESCE(a.Claims, 0)
from
  (
    select
      *
    from
      (
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
              MI2022.datedistributions_201912_20220528_010617_249
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
            MI2022.cashflows2_201912_20220528_010617_249
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
            Left(a.ContractIssueDate, 6) as CohortMonth,
            Left(a.ContractIssueDate, 4) as CohortYear,
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
            MI2022.contracts_201912_20220528_010617_249 a
            LEFT JOIN CTE0 c on (a.ContractId = c.ContractId)
            LEFT JOIN DateDistributionsTrans d on (c.DateDistributionid = d.DateDistributionid)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 g on (a.InsurerId = g.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 h on (a.InsuredId = h.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 i on (c.FromPartyId = i.InsurerId)
            LEFT JOIN MI2022.Entities_201912_20220528_010617_249 j on (c.ToPartyId = j.InsurerId)
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
            to_date(cast(CohortMonth as string), 'yyyyMM') as CohortMonth,
            to_date(cast(CohortYear as string), 'y') as CohortYear,
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
            LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 e on (
              TransactionCurrency = e.FromCurrency
              and e.ToCurrency = 'EUR'
              and e.ValuationDate = e.EffectToDate
            ) ---> Future Cashflows
            LEFT JOIN MI2022.FxRates3_201912_20220528_010617_249 f on (
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
          ValuationDate,
          CohortMonth,
          CohortYear,
          MainUnit,
          CashflowNature,
          MainProduct,
          CashflowType,
          Sum(CustomValue) as CustomValue
        from
          CTE3
        group by
          ValuationDate,
          CohortMonth,
          CohortYear,
          MainUnit,
          CashflowNature,
          CashflowType,
          MainProduct
      ) pivot (
        MAX(CustomValue) for CashflowType in ('P' as Premium, 'E' as Expenses, 'C' as Claims)
      )
  ) a

-- COMMAND ----------

desc mireporting.expenseanalysis_1_6_Agg
