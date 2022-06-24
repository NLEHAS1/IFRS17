-- Databricks notebook source
show tables in mi2022

-- COMMAND ----------

create view MI2022.StartingPointReports_201912_20220413_133443_206 as
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
  a.DataSource as ContractsDataSource,
  a.PolicyId,
  a.ManagedTogetherId,
  a.InsurerId,
  a.InsuredId,
  a.BeneficiaryId,
  a.CustomerCountry,
  a.CoverStartDate,
  a.CoverEndDate,
  a.BoundDate,
  a.ContractInceptionDate,
  a.ContractIssueDate,
  Left(a.ContractIssueDate, 4) as CohortYear,
  Left(a.ContractIssueDate, 6) as UnderwritingMonth,
  a.RiskPeriodStartDate,
  a.RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  c.FutureStateGroupId,
  c.FutureStateGroupProbability,
  c.CashFlowId,
  c.CashFlowType,
  c.CashFlowSubType,
  c.FromPartyId,
  c.ToPartyId,
  c.RiskCounterPartyId,
  c.CountryOfRisk,
  c.DataSource as CashflowsDataSource,
  c.ModelId,
  c.InvoiceId,
  c.ClaimId,
  c.FlowSequenceId,
  c.DateDistributionId,
  c.RiskCurrency,
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
),

CTE2 as (
select a.*,
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

 ContractsTypes as(
  select
    distinct a.ContractId,
    b.INSURANCE_CONTRACT_GROUP_ID,
    b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
    Case
      When a.intra_group_elimination_flg is null then 0
      else a.intra_group_elimination_flg
    End as intra_group_elimination_flg,
    a.direct_contract_flg,
    a.rein_contract_flg,
    case
      when CalculationEntityL1 is null then "None"
      Else CalculationEntityL1
    End as CalculationEntityL1,
    case
      when CalculationEntityL2 is null then "None"
      Else CalculationEntityL2
    End as CalculationEntityL2,
    case
      when CalculationEntityL3 is null then "None"
      Else CalculationEntityL3
    End as CalculationEntityL3,
    case
      when CalculationEntityL4 is null then "None"
      Else CalculationEntityL4
    End as CalculationEntityL4,
    case
      when CalculationEntityL5 is null then "None"
      Else CalculationEntityL5
    End as CalculationEntityL5
  from
    (
      select
        *
      from
        (
          select
            contractid,
            CalculationEntity,
            HierarchyLevel,
            intra_group_elimination_flg,
            direct_contract_flg,
            rein_contract_flg
          from
            Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 a
            left join (
              seleCt
                a.ValuationDate,
                case
                  When a.EntityId = b.EntityId then "CalculationEntityL1"
                  when (
                    a.EntityId != b.EntityId
                    and b.EntityId = c.EntityId
                  ) then "CalculationEntityL2"
                  when (
                    a.EntityId != b.EntityId
                    and c.EntityId = d.EntityId
                  ) then "CalculationEntityL3"
                  When (
                    a.EntityId != b.EntityId
                    and d.EntityId = e.EntityId
                  ) then "CalculationEntityL4"
                  Else "CalculationEntityL5"
                End as HierarchyLevel,
                a.EntityId,
                a.HierarchyId,
                a.ParentEntityId,
                a.IFRS17CalculationEntity,
                a.IFRS17ReportingEntity,
                a.IFRS17ConsolidationEntity
              from
                MI2022.hierarchies_201912_20220413_133443_206 a
                left join MI2022.hierarchies_201912_20220413_133443_206 b on a.ParentEntityId = b.EntityId
                left join MI2022.hierarchies_201912_20220413_133443_206 c on b.ParentEntityId = c.EntityId
                left join MI2022.hierarchies_201912_20220413_133443_206 d on c.ParentEntityId = d.EntityId
                left join MI2022.hierarchies_201912_20220413_133443_206 e on d.ParentEntityId = e.EntityId
            ) b on (a.CalculationEntity = b.Entityid)
          where
            GROUP_TYPE_CD = 'SUBGROUP'
        ) PIVOT (
          max(CalculationEntity) for HierarchyLevel in (
            'CalculationEntityL1',
            'CalculationEntityL2',
            'CalculationEntityL3',
            'CalculationEntityL4',
            'CalculationEntityL5'
          )
        )
    ) a
    Inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.contractid = b.contractid
      and a.direct_contract_flg = b.direct_contract_flg
      and a.rein_contract_flg = b.rein_contract_flg
      and GROUP_TYPE_CD = 'SUBGROUP'
    )
),

CTE3 as ( --Only Atradius N.V
  select
    a.*,
  case
    When (
      b.CalculationEntityL4 != 'None'
      and b.CalculationEntityL4 = a.FrompartyEntityId
    ) then a.AmountEUR * -1
    When (
      b.CalculationEntityL4 = 'None'
      and b.CalculationEntityL3 != 'None'
      and b.CalculationEntityL3 = a.FrompartyEntityId
    ) then a.AmountEUR * -1
    else AmountEUR
  End as CustomValue,
  case
    When b.CalculationEntityL4 != 'None' then CalculationEntityL4
    When (
      b.CalculationEntityL4 = 'None'
      and b.CalculationEntityL3 != 'None'
    ) then b.CalculationEntityL3
  End as Entity,
  b.direct_contract_flg,
  b.rein_contract_flg,
  b.intra_group_elimination_flg,
  b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
  b.INSURANCE_CONTRACT_GROUP_ID
  from
    CTE2 a
    Inner JOIN ContractsTypes b on (a.ContractId = b.ContractId)
),

CTECT as (
  select
    a.*,
    case
      when b.direct_contract_flg = 1
      and b.rein_contract_flg = 0 then "Direct Contract"
      When b.rein_contract_flg = 1
      and b.direct_contract_flg = 0 then "Reinsurance Held Contract"
      Else "IntraGroup"
    End as ContractType,
    b.CalculationEntity,
    case 
    when b.MAIN_INSURANCE_CONTRACT_GROUP_ID like '%RM%' then 'Remaining'
    when b.MAIN_INSURANCE_CONTRACT_GROUP_ID like '%ON%' then 'Onerous'
    Else 'Non Onerous'
    End as ProfitabilityClass
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (   
    a.contractid = b.contractid
    and a.direct_contract_flg = b.direct_contract_flg
    and a.rein_contract_flg = b.rein_contract_flg
    and GROUP_TYPE_CD = 'SUBGROUP'
    and a.INSURANCE_CONTRACT_GROUP_ID = b.INSURANCE_CONTRACT_GROUP_ID
    and a.MAIN_INSURANCE_CONTRACT_GROUP_ID = b.MAIN_INSURANCE_CONTRACT_GROUP_ID
    )
)
select * from CTECT
