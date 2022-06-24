-- Databricks notebook source
use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;

-- COMMAND ----------

drop view MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab_Initial

-- COMMAND ----------

Create view MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab_Initial as 
-- The CTE is transforming Groupings table to make it joinable with Contracts table
WITH GroupingsTrans as (
  select
    distinct a.ValuationDate,
    b.CalculationEntity,
    b.InsurerId,
    b.InsuredId,
    a.MainUnit,
    a.MainProduct,
    GroupingKey,
    Order
  from
    contracts a
    left join groupings b on (a.ValuationDate = b.ValuationDate)
    And (
      a.mainunit like '%GLB%'
      and left(a.mainunit, 3) = left(b.mainunit, 3)
      and a.mainproduct = b.mainproduct
    )
    OR (
      a.mainunit = b.mainunit
      and a.mainproduct = b.mainproduct
    )
    OR (
      a.mainunit like '%Group'
      and left(a.mainunit, 3) = left(b.mainunit, 3)
    )
),

-- The CTE aggregating Datedistributions table
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
      Datedistributions
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
    Datedistributionsint2),

-- The CTE isn't doing any transformations. It's for combining Entities and Hierarchies table
EntitiesHierarchies as (
  select
    a.ValuationDate,
    a.EntityId,
    InsurerId,
    Name,
    LegalName,
    EntityType,
    Country,
    InsuranceActivity,
    ReportingCurrency,
    FunctionalCurrency,
    ReportingLanguage,
    Role,
    Purpose,
    HierarchyId,
    ParentEntityId,
    IFRS17CalculationEntity,
    IFRS17ReportingEntity,
    IFRS17ConsolidationEntity
  from
    entities a
    left join hierarchies b on (
      a.entityid = b.entityid
      and a.valuationDate = b.valuationDate
    ))

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
  a.RiskPeriodStartDate,
  a.RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  b.GroupingKey,
  c.FutureStateId,
  c.FutureStateProbability,
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
    When c.TransactionCurrency = 'EUR' then e.ConversionRate
    When d.CustomCashflowDate > d.ValuationDate then e.ConversionRate ---> Future Cashflows
    When d.CustomCashflowDate <= d.ValuationDate then f.ConversionRate ---> Actual Cashflows
    Else e.ConversionRate -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as ConversionRate,
  CASE
    When c.TransactionCurrency = 'EUR' then c.Amount * d.weight
    When (
      c.TransactionCurrency != 'EUR'
      and d.CustomCashflowDate > d.ValuationDate
    ) then c.Amount * e.ConversionRate * d.weight ---> Future Cashflows
    When (
      c.TransactionCurrency != 'EUR'
      and d.CustomCashflowDate <= d.ValuationDate
    ) then c.Amount * f.ConversionRate * d.weight ---> Actual Cashflows
    Else c.Amount * e.ConversionRate * d.weight -- This condition will make sure that currencies at cashflowdate that don't have exhange rate will take the exchange rate at Valuation Date
  End as AmountEUR,
  CASE
    when d.CustomCashflowDate > d.ValuationDate then "Future Cashflow"
    Else "Actual Cashflow"
  End as CashflowNature,
  g.EntityId as InsurerEntityId,
  g.InsurerId as InsurerInsurerId,
  g.Name as InsurerName,
  g.LegalName as InsurerLegalName,
  g.EntityType as InsurerEntityType,
  g.Country as InsurerCountry,
  g.InsuranceActivity as InsurerInsuranceActivity,
  g.ReportingCurrency as InsurerReportingCurrency,
  g.FunctionalCurrency as InsurerFunctionalCurrency,
  g.ReportingLanguage as InsurerReportingLanguage,
  g.Role as InsurerRole,
  g.Purpose as InsurerPurpose,
  g.HierarchyId as InsurerHierarchyId,
  g.ParentEntityId as InsurerParentEntityId,
  g.IFRS17CalculationEntity as InsurerIFRS17CalculationEntity,
  g.IFRS17ReportingEntity as InsurerIFRS17ReportingEntity,
  g.IFRS17ConsolidationEntity as InsurerIFRS17ConsolidationEntity,
  h.EntityId as InsuredEntityId,
  h.InsurerId as InsuredInsurerId,
  h.Name as InsuredName,
  h.LegalName as InsuredLegalName,
  h.EntityType as InsuredEntityType,
  h.Country as InsuredCountry,
  h.InsuranceActivity as InsuredInsuranceActivity,
  h.ReportingCurrency as InsuredReportingCurrency,
  h.FunctionalCurrency as InsuredFunctionalCurrency,
  h.ReportingLanguage as InsuredReportingLanguage,
  h.Role as InsuredRole,
  h.Purpose as InsuredPurpose,
  h.HierarchyId as InsuredHierarchyId,
  h.ParentEntityId as InsuredParentEntityId,
  h.IFRS17CalculationEntity as InsuredIFRS17CalculationEntity,
  h.IFRS17ReportingEntity as InsuredIFRS17ReportingEntity,
  h.IFRS17ConsolidationEntity as InsuredIFRS17ConsolidationEntity,
  i.EntityId as FrompartyEntityId,
  i.InsurerId as FrompartyInsurerId,
  i.Name as FrompartyName,
  i.LegalName as FrompartyLegalName,
  i.EntityType as FrompartyEntityType,
  i.Country as FrompartyCountry,
  i.InsuranceActivity as FrompartyInsuranceActivity,
  i.ReportingCurrency as FrompartyReportingCurrency,
  i.FunctionalCurrency as FrompartyFunctionalCurrency,
  i.ReportingLanguage as FrompartyReportingLanguage,
  i.Role as FrompartyRole,
  i.Purpose as FrompartyPurpose,
  i.HierarchyId as FrompartyHierarchyId,
  i.ParentEntityId as FrompartyParentEntityId,
  i.IFRS17CalculationEntity as FrompartyIFRS17CalculationEntity,
  i.IFRS17ReportingEntity as FrompartyIFRS17ReportingEntity,
  i.IFRS17ConsolidationEntity as FrompartyIFRS17ConsolidationEntity,
  j.EntityId as TopartyEntityId,
  j.InsurerId as TopartyInsurerId,
  j.Name as TopartyName,
  j.LegalName as TopartyLegalName,
  j.EntityType as TopartyEntityType,
  j.Country as TopartyCountry,
  j.InsuranceActivity as TopartyInsuranceActivity,
  j.ReportingCurrency as TopartyReportingCurrency,
  j.FunctionalCurrency as TopartyFunctionalCurrency,
  j.ReportingLanguage as TopartyReportingLanguage,
  j.Role as TopartyRole,
  j.Purpose as TopartyPurpose,
  j.HierarchyId as TopartyHierarchyId,
  j.ParentEntityId as TopartyParentEntityId,
  j.IFRS17CalculationEntity as TopartyIFRS17CalculationEntity,
  j.IFRS17ReportingEntity as TopartyIFRS17ReportingEntity,
  j.IFRS17ConsolidationEntity as TopartyIFRS17ConsolidationEntity,
  case
    when a.PolicyId = n.id then n.ORCUR_ORNNN_ID
    when a.policyid = p.co_poliza then p.nu_nif_empresa
    Else 'None'
  End as CustomerID,
  case
    when a.PolicyId = n.id then o.d_ornol_short_name
    when a.policyid = p.co_poliza then p.no_empresa
    Else 'None'
  End as CustomerName,
  SUBSTRING_INDEX(modelid, "_", 1) as Model --
FROM
  Contracts a
  LEFT JOIN GroupingsTrans b on (
    a.mainUnit = b.mainunit
    and a.mainproduct = b.mainproduct
    and a.ValuationDate = b.ValuationDate
  )
  LEFT JOIN Cashflows c on (
    a.ContractId = c.ContractId
    and a.ValuationDate = c.ValuationDate
  )
  LEFT JOIN DateDistributionsTrans d on (
    c.DateDistributionid = d.DateDistributionid
    and c.ValuationDate = d.ValuationDate
  )
  -- The only transformation I did for FxRates is to add Euro to Euro exchange rate
  -- The first join of FxRates3 is to exctract the right conversion rate for Future Cashflows
  LEFT JOIN FxRates3 e on (
    c.TransactionCurrency = e.FromCurrency
    and e.ToCurrency = 'EUR'
    and e.ValuationDate = e.EffectToDate
  ) ---> Future Cashflows
  -- The second join of FxRates3 is to exctract the right conversion rate for Actual Cashflows
  LEFT JOIN FxRates3 f on (
    c.TransactionCurrency = f.FromCurrency
    and f.ToCurrency = 'EUR'
    and d.CustomCashflowDate = f.EffectToDate
  ) ---> Actual Cashflows
  LEFT JOIN EntitiesHierarchies g on (
    a.InsurerId = g.InsurerId
    and a.ValuationDate = g.ValuationDate
  )
  LEFT JOIN EntitiesHierarchies h on (
    a.InsuredId = h.InsurerId
    and a.ValuationDate = h.ValuationDate
  )
  LEFT JOIN EntitiesHierarchies i on (
    c.FromPartyId = i.InsurerId
    and c.ValuationDate = i.ValuationDate
  )
  LEFT JOIN EntitiesHierarchies j on (
    c.ToPartyId = j.InsurerId
    and c.ValuationDate = j.ValuationDate
  )
  LEFT JOIN MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 n on (a.PolicyId = n.id)
  left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 o on (
    n.ORCUR_ORNNN_ID = o.id
    and o.effect_to_dat > current_date()
  )
  LEFT JOIN mi2022.asegurado_20191231 p on (
    a.policyid = p.co_poliza
    and p.current = 'Y'
  )

-- COMMAND ----------

-- MAGIC %md I'm creating another view on top of the previous view because the Amount in Euro is needed in subsequent calculations

-- COMMAND ----------

drop view MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab

-- COMMAND ----------

Create view MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab as 
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
            db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.dcrhc a
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
                hierarchies a
                left join hierarchies b on a.ParentEntityId = b.EntityId
                left join hierarchies c on b.ParentEntityId = c.EntityId
                left join hierarchies d on c.ParentEntityId = d.EntityId
                left join hierarchies e on d.ParentEntityId = e.EntityId
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
    left join db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.dcrhc b on (
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
  MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab_Initial a
  LEFT JOIN ContractsTypes b on (a.ContractId = b.ContractId)
  LEFT JOIN db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.dcrhc c on (
    a.contractid = c.contractid
    and b.direct_contract_flg = c.direct_contract_flg
    and b.rein_contract_flg = c.rein_contract_flg
    and c.GROUP_TYPE_CD = 'SUBGROUP'
    and b.INSURANCE_CONTRACT_GROUP_ID = c.INSURANCE_CONTRACT_GROUP_ID
    and b.MAIN_INSURANCE_CONTRACT_GROUP_ID = c.MAIN_INSURANCE_CONTRACT_GROUP_ID
  )

-- COMMAND ----------

select * from ContractsTypes

-- COMMAND ----------

select * from MI2022.StartingPointReports_2021_10_13_192247_fullpatched_4fab where contractid = 'SYM / 1017837 / 20190701'

-- COMMAND ----------

-- MAGIC %md #Transformations

-- COMMAND ----------

-- MAGIC %md ###1.Groupings

-- COMMAND ----------

select
  distinct a.ValuationDate,
  b.CalculationEntity,
  b.InsurerId,
  b.InsuredId,
  a.MainUnit,
  a.MainProduct,
  GroupingKey,
  Order
from
  contracts a
  left join groupings b on (a.ValuationDate = b.ValuationDate)
  And (
    a.mainunit like '%GLB%'
    and left(a.mainunit, 3) = left(b.mainunit, 3)
    and a.mainproduct = b.mainproduct
  )
  OR (
    a.mainunit = b.mainunit
    and a.mainproduct = b.mainproduct
  )
  OR (
    a.mainunit like '%Group'
    and left(a.mainunit, 3) = left(b.mainunit, 3)
  )

-- COMMAND ----------

-- MAGIC %md ###2.DateDistributions

-- COMMAND ----------

with Datedistributionsint as 
  (select 
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
from Datedistributions
  ),
  
 Datedistributionsint2 as
(select
  DateDistributionid,
  ValuationDate,
  Case
  When weight is null  then CashflowDate
  when weight=1 then CashflowDate
  When weight!=1 and CashflowDate>Valuationdate then 47121231 --> ExpectedCashflow
  When weight!=1 and CashflowDate <= Valuationdate then 00000000
  End as CustomCashflowDate,
  sum(weight) as weight
from
  Datedistributionsint
group by
DateDistributionid,
ValuationDate,
CustomCashflowDate)
select
  DateDistributionid,
  ValuationDate,
  CustomCashflowDate,
  cast(weight as int) as Weight
  from
  Datedistributionsint2

-- COMMAND ----------

-- MAGIC %md #3.FxRates

-- COMMAND ----------

insert into FxRates values 
(ValuationDate, ValuationDate, ValuationDate, '*','EUR', 'EUR',1,'None','None','IFRS17')  

-- COMMAND ----------

-- MAGIC %md Entities & Hierarchies

-- COMMAND ----------

select
    a.ValuationDate,
    a.EntityId,
    InsurerId,
    Name,
    LegalName,
    EntityType,
    Country,
    InsuranceActivity,
    ReportingCurrency,
    FunctionalCurrency,
    ReportingLanguage,
    Role,
    Purpose,
    HierarchyId,
    ParentEntityId,
    IFRS17CalculationEntity,
    IFRS17ReportingEntity,
    IFRS17ConsolidationEntity
  from
    entities a
    left join hierarchies b on (
      a.entityid = b.entityid
      and a.valuationDate = b.valuationDate
    )

-- COMMAND ----------

-- MAGIC %md #4.ContractsTypes

-- COMMAND ----------

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
  (select * from ( select contractid,CalculationEntity, 
HierarchyLevel,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.dcrhc a left join (seleCt
a.ValuationDate,
case
When a.EntityId=b.EntityId then "CalculationEntityL1"
when (a.EntityId!=b.EntityId and b.EntityId=c.EntityId) then "CalculationEntityL2"
when (a.EntityId!=b.EntityId and c.EntityId=d.EntityId) then "CalculationEntityL3"
When (a.EntityId!=b.EntityId and d.EntityId=e.EntityId) then "CalculationEntityL4"
Else "CalculationEntityL5"
End as HierarchyLevel,
a.EntityId,
a.HierarchyId,
a.ParentEntityId,
a.IFRS17CalculationEntity,
a.IFRS17ReportingEntity,
a.IFRS17ConsolidationEntity
from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId) b on (a.CalculationEntity=b.Entityid) where GROUP_TYPE_CD='SUBGROUP') 
          PIVOT (
            max(CalculationEntity) for HierarchyLevel in ('CalculationEntityL1', 'CalculationEntityL2', 'CalculationEntityL3', 'CalculationEntityL4','CalculationEntityL5'))) a
  left join db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.dcrhc b on (
    a.contractid = b.contractid
    and a.direct_contract_flg = b.direct_contract_flg
    and a.rein_contract_flg = b.rein_contract_flg
    and GROUP_TYPE_CD = 'SUBGROUP')

-- COMMAND ----------

SYM / 1017837 / 20190701
