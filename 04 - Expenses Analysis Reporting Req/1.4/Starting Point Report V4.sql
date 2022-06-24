-- Databricks notebook source
drop view MI2022.ExpenseAnalysisStep1

-- COMMAND ----------

create view MI2022.ExpenseAnalysisStep1 as
select
  distinct a.ValuationDate,
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
  c.Amount,
  e.ConversionRate,
  c.Amount * e.ConversionRate as AmountEUR,
  g.EntityId as InsurerEntityId,
  g.Name as InsurerName,
  h.EntityId as InsuredEntityId,
  h.Name as InsuredName,
  i.EntityId as FrompartyEntityId,
  i.Name as FrompartyName,
  j.EntityId as TopartyEntityId,
  j.Name as TopartyName
  FROM
  MI2022.contracts_201912_20220413_133443_206 a
  LEFT JOIN MI2022.cashflows_201912_20220413_133443_206 c on (
    a.ContractId = c.ContractId
  )
  -- The only transformation I did for FxRates is to add Euro to Euro exchange rate
  -- The first join of FxRates3 is to exctract the right conversion rate for Future Cashflows
  LEFT JOIN MI2022.FxRates3_201912_20220413_133443_206 e on (
    c.TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 g on (
    a.InsurerId = g.InsurerId
  )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 h on (
    a.InsuredId = h.InsurerId
  )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 i on (
    c.FromPartyId = i.InsurerId
  )
  LEFT JOIN MI2022.Entities_201912_20220413_133443_206 j on (
    c.ToPartyId = j.InsurerId
  )


-- COMMAND ----------

Create view MI2022.ExpenseAnalysisStep2 as 
With ContractsTypes as(
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
    left join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.contractid = b.contractid
      and a.direct_contract_flg = b.direct_contract_flg
      and a.rein_contract_flg = b.rein_contract_flg
      and GROUP_TYPE_CD = 'SUBGROUP'
    )
)
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
  b.direct_contract_flg as DirectContractFlg,
  b.rein_contract_flg as ReinContractFlg,
  case
    when b.direct_contract_flg = 1
    and b.rein_contract_flg = 0 then "Direct Contract"
    When b.rein_contract_flg = 1
    and b.direct_contract_flg = 0 then "Reinsurance Held Contract"
  End as ContractType,
  b.intra_group_elimination_flg as IntraGroupEliminationFlg,
  c.CalculationEntity,
  b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
  b.INSURANCE_CONTRACT_GROUP_ID,
  case 
  when b.MAIN_INSURANCE_CONTRACT_GROUP_ID like '%RM%' then 'Remaining'
  when b.MAIN_INSURANCE_CONTRACT_GROUP_ID like '%ON%' then 'Onerous'
  Else 'Non Onerous'
  End as ProfitabilityClass
From
  MI2022.ExpenseAnalysisStep1 a
  Inner JOIN ContractsTypes b on (a.ContractId = b.ContractId) --Inner Join to only process the ContractIds in SAS
  Inner JOIN Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 c on ( --Inner Join to only process the ContractIds in SAS
    a.contractid = c.contractid
    and b.direct_contract_flg = c.direct_contract_flg
    and b.rein_contract_flg = c.rein_contract_flg
    and c.GROUP_TYPE_CD = 'SUBGROUP'
    and b.INSURANCE_CONTRACT_GROUP_ID = c.INSURANCE_CONTRACT_GROUP_ID
    and b.MAIN_INSURANCE_CONTRACT_GROUP_ID = c.MAIN_INSURANCE_CONTRACT_GROUP_ID
  )

-- COMMAND ----------

With TotalPrem as
(select 
CohortYear,
ContractType,
Sum(CustomValue) as Premium
from MI2022.ExpenseAnalysisStep2
where Cashflowtype='P' and CohortYear>2000 and CalculationEntity='001'
group by CohortYear, ContractType
),

TotalExp as 
(select 
CohortYear,
ContractType,
Sum(CustomValue) as Expenses
from MI2022.ExpenseAnalysisStep2
where Cashflowtype='E' and CohortYear>2000 and CalculationEntity='001'
group by CohortYear,ContractType
),

TotalCla as
(select 
CohortYear,
ContractType,
Sum(CustomValue) as Claims
from MI2022.ExpenseAnalysisStep2
where Cashflowtype='C' and CohortYear>2000 and CalculationEntity='001'
group by CohortYear,ContractType
)
select p.CohortYear, p.ContractType, Premium, Expenses, Claims,
Expenses/Premium as ExpenseRatio,
Claims/Premium as ClaimsRatio
from
TotalPrem p full join TotalExp e on (p.CohortYear=e.CohortYear and p.ContractType=e.ContractType)
full join TotalCla c on (p.CohortYear=c.CohortYear and p.ContractType=c.ContractType)

-- COMMAND ----------

drop view MI2022.ExpenseAnalysisAgg

-- COMMAND ----------

Create view MI2022.ExpenseAnalysisAgg as
with CTE1 as (
  select
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    Left(a.ContractIssueDate, 4) as CohortYear,
    Sum(amount) as Amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid = b.contractid
  where
    Left(a.ContractIssueDate, 4) > 2000
  group by
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CohortYear
),
CTE2 as (
  select
    a.*,
    a.Amount * b.ConversionRate as AmountEUR
  from
    CTE1 a
    left join MI2022.FxRates3_201912_20220413_133443_206 b on (
      a.TransactionCurrency = b.FromCurrency
      and b.ToCurrency = 'EUR'
      and b.ValuationDate = b.EffectToDate
    )
),
CTE3 as (
  select
    a.*,
    case
      When InsurerId = FromPartyId then AmountEUR * -1
      Else AmountEUR
    End as CustomValue
  from
    CTE2 a
),
CTECT as (
  select
    a.*,
    case
      when b.direct_contract_flg = 1
      and b.rein_contract_flg = 0 then "Direct Contract"
      When b.rein_contract_flg = 1
      and b.direct_contract_flg = 0 then "Reinsurance Held Contract"
    End as ContractType
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.Contractid = b.contractid
      and GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001
    )
),
CTEP as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Premium
  from
    CTECT
  where
    Cashflowtype = 'P'
  group by
    CohortYear,
    ContractType
),
CTEE as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Expenses
  from
    CTECT
  where
    Cashflowtype = 'E'
  group by
    CohortYear,
    ContractType
),
CTEC as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Claims
  from
    CTECT
  where
    Cashflowtype = 'C'
  group by
    CohortYear,
    ContractType
)
select
  p.CohortYear,
  p.ContractType,
  Premium,
  Expenses,
  Claims,
  Expenses / Premium as ExpenseRatio,
  Claims / Premium as ClaimsRatio,
  (Expenses / Premium)+(Claims / Premium) as CombinedRatio,
1+(Expenses / Premium)+(Claims / Premium) as Margin
from
  CTEP p inner
  join CTEE e on (
    p.CohortYear = e.CohortYear
    and p.ContractType = e.ContractType
  ) inner
  join CTEC c on (
    p.CohortYear = c.CohortYear
    and p.ContractType = c.ContractType
  )

-- COMMAND ----------

select * from MI2022.ExpenseAnalysisAgg order by cohortyear desc

-- COMMAND ----------

-- MAGIC %md Validation

-- COMMAND ----------

with CTE1 as (
  select
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    Left(a.ContractIssueDate, 4) as CohortYear,
    Sum(amount) as Amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid = b.contractid
  where
    Left(a.ContractIssueDate, 4) > 2000
  group by
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CohortYear
),
CTE2 as (
  select
    a.*,
    a.Amount * b.ConversionRate as AmountEUR
  from
    CTE1 a
    left join MI2022.FxRates3_201912_20220413_133443_206 b on (
      a.TransactionCurrency = b.FromCurrency
      and b.ToCurrency = 'EUR'
      and b.ValuationDate = b.EffectToDate
    )
),
CTE3 as (
  select
    a.*,
    case
      When InsurerId = FromPartyId then AmountEUR * -1
      Else AmountEUR
    End as CustomValue
  from
    CTE2 a
),
CTECT as (
  select
    a.*,
    case
      when b.direct_contract_flg = 1
      and b.rein_contract_flg = 0 then "Direct Contract"
      When b.rein_contract_flg = 1
      and b.direct_contract_flg = 0 then "Reinsurance Held Contract"
    End as ContractType
  from
    CTE3 a
    inner join Mi2022.CONTRACT_TYPE_201912_20220413_133443_206 b on (
      a.Contractid = b.contractid
      and GROUP_TYPE_CD = 'SUBGROUP'
      and calculationentity = 001
    )
),
CTEP as (
  select
    CohortYear,
    ContractType,
    Sum(CustomValue) as Premium
  from
    CTECT
  where
    Cashflowtype = 'P'
  group by
    CohortYear,
    ContractType
)
select * from CTEP where cohortyear=2020

-- COMMAND ----------

with CTE1 as (
  select
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    Left(a.ContractIssueDate, 4) as CohortYear,
    Sum(amount) as Amount
  from
    MI2022.contracts_201912_20220413_133443_206 a
    left join MI2022.cashflows_201912_20220413_133443_206 b on a.contractid = b.contractid
  where
    Left(a.ContractIssueDate, 4) > 2000
  group by
    a.ContractId,
    CashflowType,
    TransactionCurrency,
    InsurerId,
    FromPartyId,
    CohortYear
),
CTE2 as (
  select
    a.*,
    a.Amount * b.ConversionRate as AmountEUR
  from
    CTE1 a
    left join MI2022.FxRates3_201912_20220413_133443_206 b on (
      a.TransactionCurrency = b.FromCurrency
      and b.ToCurrency = 'EUR'
      and b.ValuationDate = b.EffectToDate
    )
)
select * from CTE2

-- COMMAND ----------

 select
    a.*,
    a.Amount * b.ConversionRate as AmountEUR
  from
    MI2022.cashflows_201912_20220413_133443_206 a
    left join MI2022.FxRates3_201912_20220413_133443_206 b on (
      a.TransactionCurrency = b.FromCurrency
      and b.ToCurrency = 'EUR'
      and b.ValuationDate = b.EffectToDate)
      where TransactionCurrency!='EUR'

-- COMMAND ----------

select * from MI2022.FxRates3_201912_20220413_133443_206 where tocurrency='EUR' and fromcurrency='PYG' and effectfromdate=20191231
