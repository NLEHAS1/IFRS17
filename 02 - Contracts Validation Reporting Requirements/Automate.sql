-- Databricks notebook source
select * from mi2022.control1a_20200331_v20220529_01

-- COMMAND ----------

CREATE WIDGET DROPDOWN Databases DEFAULT "db_2021_11_16_2020q4_metadatafix_patched_v3_cons_nofilter_c0c56e3522820f74065a89e0ab369074baaaf905" CHOICES show databases

-- COMMAND ----------

select '$Databases' as schemaname, a.* from $Databases.Cashflows a

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

Use $Databases; show tables

-- COMMAND ----------

-- MAGIC %md #Control1a

-- COMMAND ----------

Create view $Databases.Control1a as 
with MaximumCreditTerms as (
  Select
    pvs.Bupiy_Id,
    pvs.Start_Risk_Dat,
    pvs.End_Risk_Dat,
    MAx(Effect_from_Dat) as Effect_from_Dat,
    MAx(Effect_To_Dat) as Effect_To_Dat,
    MAX(MAX_Credit_Terms_PER) as MaximumCreditTerms,
    MAX(MAx_Credit_terms_per_typ) as MaximumCreditTermsType,
    MAX(MAX_CRedit_terms2_per) as DeviatingMaximumCreditTerms,
    MAx(MAX_CREDIT_TERMS2_PER_TYP) as DeviatingMaximumCreditTermsType
  From
    $Databases.TBPO_POL_VERSIONS pvs
    Left Join $Databases.TBPO_CTRY_GRP_MCTS ctry On pvs.bupiy_id = ctry.Bupiy_id
    And pvs.end_risk_dat between ctry.effect_from_DAT
    and ctry.effect_To_Dat
  Group By
    pvs.Bupiy_Id,
    pvs.Start_Risk_Dat,
    pvs.End_Risk_Dat
  ORDER BY
    2 ASC NULLS LAST
),
MaximumCreditPeriod as (
  SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType,
    pol.start_risk_dat
  FROM
    $Databases.TBPO_POL_VERSIONS pol
    JOIN $Databases.TBPO_CTRY_GRP_MCTS ctrygrp on (
      ctrygrp.bupiy_id = pol.Bupiy_id
      AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
      AND ctrygrp.effect_to_dat
    )
  where
    ctrygrp.max_cred_per is not null
    and ctrygrp.max_cred_per_typ is not null
  GROUP BY
    pol.bupiy_id,
    ctrygrp.max_cred_per_typ,
    pol.start_risk_dat
)
Select
  distinct a.ValuationDate,
  a.PolicyId,
  a.ContractId,
  a.DataSource,
  a.ManagedTogetherId,
  a.InsurerId,
  a.InsuredId,
  a.BeneficiaryId,
  a.CustomerCountry,
  to_date(cast(a.CoverEndDate as string), 'yyyyMMdd') as CoverEndDate,
  to_date(cast(a.CoverStartDate as string), 'yyyyMMdd') as CoverStartDate,
  DATEDIFF(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) as CoveragePeriodDaysDiff,
  --step1
  months_between(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) as CoveragePeriodMontDiff,
  --step1
  DATEDIFF(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) / 360 as CoveragePeriodYearsDiff,
  --step1
  to_date(cast(a.BoundDate as string), 'yyyyMMdd') as BoundDate,
  to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') as ContractIssueDate,
  Left(a.ContractIssueDate, 4) as CohortYear,
  to_date(
    cast(a.ContractInceptionDate as string),
    'yyyyMMdd'
  ) as ContractInceptionDate,
  to_date(
    cast(a.RiskPeriodStartDate as string),
    'yyyyMMdd'
  ) as RiskPeriodStartDate,
  to_date(cast(a.RiskPeriodEndDate as string), 'yyyyMMdd') as RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  mod.porce_Code as Component_Code,
  --step0
  mod.porme_code as Module_Code,
  --step0
  refmodvar.porve_code as Variable_Code,
  --step0
  b.MaximumCreditTerms,
  b.MaximumCreditTermsType,
  c.MaximumCreditPeriod,
  c.MaximumCreditPeriodTermsType,
  b.DeviatingMaximumCreditTerms,
  b.DeviatingMaximumCreditTermsType,
  e.WaitingPeriod,
  e.WaitingPeriodType,
  Case
    When i.d_popvn_start_risk_dat = i.d_popvn_end_risk_dat then True
    Else False
  End as NoCoverPeriod,
  modvar.effect_from_dat,
  --step0
  modvar.effect_to_dat,
  --step0
  modvar.value,
  --step0
  modvar.orsus_id as UserId_Last_Update,
  --step0
  modvar.last_update_dat,
  --step0
  modvar.orsus_create_id as UserId_Created,
  modvar.create_dat,
  --step0
  modvar.change_dat,
  --step0
  modvar.seq as Sequence,
  --step0
  modvar.origin_ind,
  --step0
  modvar.unicode_value as Variable_Value,
  --step0
  f.PreCreditPeriodType_EffectFromDate,
  f.PreCreditPeriodType_EffectToDate,
  f.PreCreditPeriodType_PorceCode,
  f.PreCreditPeriodTypePormeCode,
  f.MaxCrd_Typ as Max_PCR_Period_Type,
  f.MaxCrd_Value as Max_PCR_Period,
  f.MaxCrd_PerfDat,
  f.SelPCRCover as Selective_PCR_cover,
  i.d_popss_status_code as Latest_Status_Code,
  g.start_risk_dat as PolicyStartDate,
  --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate
  g.end_risk_dat as PolicyEndDate -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate
From
  $Databases.Contracts a
  left join $Databases.TBPO_POL_MODULES mod on mod.Bupiy_Id = a.PolicyId
  Left Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES refmodvar On (
    mod.porce_code = refmodvar.porce_code
    and mod.porme_Code = refmodvar.porme_code
  )
  Left Join $Databases.TBPO_POL_MOD_VARIABLES modvar On (
    refmodvar.porve_Code = modvar.porve_code
    and mod.bupiy_id = modVar.Bupiy_id
  ) --Drivers
  --MaximumCreditTerms
  left join MaximumCreditTerms b on (
    a.policyid = b.Bupiy_Id
    and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') between to_date(b.effect_from_dat, 'yyyyMMdd')
    and to_date(b.effect_to_dat, 'yyyyMMdd')
    and to_date(cast(a.contractissuedate as string), 'yyyyMMdd') = to_date(b.Start_Risk_Dat, 'yyyyMMdd')
  ) --MaximumCreditPeriod
  left join MaximumCreditPeriod c on (
    a.policyid = c.PolicyId
    and to_date(c.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  ) --WaitingPeriod
  Left Join (
    SELECT
      pol.bupiy_id as PolicyId,
      max(ctrygrpcvr.waiting_period) as WaitingPeriod,
      ctrygrpcvr.waiting_period_typ as WaitingPeriodType
    FROM
      $Databases.TBPO_POL_VERSIONS pol
      JOIN $Databases.TBPO_CTRY_GRP_COVER_PCTS ctrygrpcvr on (
        ctrygrpcvr.bupiy_id = pol.bupiy_id
        AND pol.end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat
        AND ctrygrpcvr.effect_to_dat
      )
    Where
      Waiting_period <> 0
    GROUP BY
      pol.Bupiy_id,
      ctrygrpcvr.waiting_period_typ
  ) e on a.PolicyId = e.PolicyId --PreCreditPeriodType
  left join (
    select
      *
    from
      (
        Select
          pmod.Bupiy_Id as PolicyId,
          pmod.Effect_from_dat as PreCreditPeriodType_EffectFromDate,
          pmod.Effect_to_Dat as PreCreditPeriodType_EffectToDate,
          pmod.Porce_code as PreCreditPeriodType_PorceCode,
          pmod.porme_code as PreCreditPeriodTypePormeCode,
          mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
          --mvar.EFFECT_FROM_DAT, -->redundanet
          --mvar.Effect_to_dat, -->redundant
          mvar.value as PreCreditPeriodTypeValue
        From
          $Databases.TBBU_POLICIES a
          left join $Databases.TBPO_POL_MODULES pmod on pmod.bupiy_id = a.id
          Left Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES mref On pmod.porme_code = mref.porme_code
          And pmod.porce_code = mref.Porce_code
          Left Join $Databases.TBPO_POL_MOD_VARIABLES mvar On pmod.Bupiy_Id = mvar.Bupiy_Id
          And mref.Porve_code = mvar.porve_Code
        Where
          a.d_popvn_end_risk_dat between pmod.effect_from_dat
          and pmod.effect_to_dat
          And a.d_popvn_end_risk_dat between mvar.effect_from_dat
          and mvar.effect_to_dat
          And (
            pmod.PORME_CODE like ('43500.%')
            OR pmod.PORME_CODE like ('43800.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44405.%')
            OR pmod.PORME_CODE like ('44400.%')
            OR pmod.PORME_CODE like ('44410.%')
          )
      ) pivot (
        max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in (
          'PERTYP3' as MaxCrd_Typ,
          'MAXPRCR' as MaxCrd_Value,
          'PERFDAT' as MaxCrd_PerfDat,
          'SELPCRCO' as SelPCRCover
        )
      )
  ) f on a.PolicyId = f.PolicyId --NoCoverPeriod
  Left Join $Databases.TBBU_POLICIES i on a.policyid = i.id
  left join $Databases.TBPO_POL_VERSIONS g on (
    a.policyid = g.bupiy_id
    and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  )
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat
  And a.RiskPeriodEndDate>a.ValuationDate
  And a.DataSource = 'SYM'

-- COMMAND ----------

-- MAGIC %md #Control1b

-- COMMAND ----------

Create view $Databases.Control1b as 
with MaximumCreditTerms as (
  Select
    pvs.Bupiy_Id,
    pvs.Start_Risk_Dat,
    pvs.End_Risk_Dat,
    MAx(Effect_from_Dat) as Effect_from_Dat,
    MAx(Effect_To_Dat) as Effect_To_Dat,
    MAX(MAX_Credit_Terms_PER) as MaximumCreditTerms,
    MAX(MAx_Credit_terms_per_typ) as MaximumCreditTermsType,
    MAX(MAX_CRedit_terms2_per) as DeviatingMaximumCreditTerms,
    MAx(MAX_CREDIT_TERMS2_PER_TYP) as DeviatingMaximumCreditTermsType
  From
    $Databases.TBPO_POL_VERSIONS pvs
    Left Join $Databases.TBPO_CTRY_GRP_MCTS ctry On pvs.bupiy_id = ctry.Bupiy_id
    And pvs.end_risk_dat between ctry.effect_from_DAT
    and ctry.effect_To_Dat
  Group By
    pvs.Bupiy_Id,
    pvs.Start_Risk_Dat,
    pvs.End_Risk_Dat
  ORDER BY
    2 ASC NULLS LAST
),
MaximumCreditPeriod as (
  SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType,
    pol.start_risk_dat
  FROM
    $Databases.TBPO_POL_VERSIONS pol
    JOIN $Databases.TBPO_CTRY_GRP_MCTS ctrygrp on (
      ctrygrp.bupiy_id = pol.Bupiy_id
      AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
      AND ctrygrp.effect_to_dat
    )
  where
    ctrygrp.max_cred_per is not null
    and ctrygrp.max_cred_per_typ is not null
  GROUP BY
    pol.bupiy_id,
    ctrygrp.max_cred_per_typ,
    pol.start_risk_dat
),
RunoffCover as (
  Select
    pvar.bupiy_Id as PolicyId,
    pol.start_risk_dat,
    refmodvar.porce_code as RunoffCoverPorceCode,
    refmodvar.porme_code as RunoffCoverPormeCode,
    pvar.porve_Code as RunoffCoverPorveCode,
    pvar.Value as RunoffCoverValue,
    pvar.last_update_dat as RunoffCoverLastUpdateDat
  From
    $Databases.TBPO_POL_VERSIONS pol
    left join $Databases.TBPO_POL_MOD_VARIABLES pvar on (
      pol.bupiy_Id = pvar.bupiy_Id
      and pol.end_risk_dat BETWEEN pvar.effect_from_dat
      AND pvar.effect_to_dat
    )
    Left Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES refmodvar On pvar.porve_code = refmodvar.porve_code
  Where
    (
     -- refmodvar.porme_code LIKE ('%04200%') Asked by Uli to be removed
     -- OR refmodvar.porme_code LIKE ('%04600%') Asked by Uli to be removed
     -- OR refmodvar.porme_code LIKE ('%04700%') Asked by Uli to be removed
     -- OR refmodvar.porme_code LIKE ('%04800%') Asked by Uli to be removed
       refmodvar.porme_code LIKE ('%05000%')
      OR refmodvar.porme_code LIKE ('%05001%')
      OR refmodvar.porme_code LIKE ('%05005%')
      OR refmodvar.porme_code LIKE ('%0510%')
      OR refmodvar.porme_code LIKE ('%05100%')
    )
  Group By
    pvar.bupiy_Id,
    refmodvar.porce_code,
    refmodvar.porme_code,
    pvar.porve_Code,
    pvar.Value,
    pvar.last_update_dat,
    pol.start_risk_dat
  Having
    pvar.last_update_dat = max(pvar.last_update_dat)
)
Select
  distinct a.ValuationDate,
  a.PolicyId,
  a.ContractId,
  a.DataSource,
  a.ManagedTogetherId,
  a.InsurerId,
  a.InsuredId,
  a.BeneficiaryId,
  a.CustomerCountry,
  to_date(cast(a.CoverEndDate as string), 'yyyyMMdd') as CoverEndDate,
  to_date(cast(a.CoverStartDate as string), 'yyyyMMdd') as CoverStartDate,
  DATEDIFF(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) as CoveragePeriodDaysDiff,
  months_between(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) as CoveragePeriodMontDiff,
  DATEDIFF(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) / 360 as CoveragePeriodYearsDiff,
  to_date(cast(a.BoundDate as string), 'yyyyMMdd') as BoundDate,
  to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') as ContractIssueDate,
  Left(a.ContractIssueDate, 4) as CohortYear,
  to_date(
    cast(a.ContractInceptionDate as string),
    'yyyyMMdd'
  ) as ContractInceptionDate,
  to_date(
    cast(a.RiskPeriodStartDate as string),
    'yyyyMMdd'
  ) as RiskPeriodStartDate,
  to_date(cast(a.RiskPeriodEndDate as string), 'yyyyMMdd') as RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  mod.porce_Code as Component_Code,
  mod.porme_code as Module_Code,
  refmodvar.porve_code,
  b.MaximumCreditTerms,
  b.MaximumCreditTermsType,
  c.MaximumCreditPeriod,
  c.MaximumCreditPeriodTermsType,
  b.DeviatingMaximumCreditTerms,
  b.DeviatingMaximumCreditTermsType,
  e.WaitingPeriod,
  e.WaitingPeriodType,
  Case
    When i.d_popvn_start_risk_dat = i.d_popvn_end_risk_dat then True
    Else False
  End as NoCoverPeriod,
  --f.PreCreditPeriodTypePorceCode,
  --f.PreCreditPeriodTypePormecode,
  --f.PreCreditPeriodTypeProveCode,
  --f.PreCreditPeriodTypeValue,
  --f.PreCreditPeriodTypeLastUpdateDate,
  modvar.effect_from_dat,
  modvar.effect_to_dat,
  modvar.value,
  modvar.orsus_id as UserId_Last_Update,
  modvar.last_update_dat,
  modvar.orsus_create_id as UserId_Created,
  modvar.create_dat,
  modvar.change_dat,
  modvar.seq as Sequence,
  modvar.origin_ind,
  modvar.unicode_value as Variable_Value,
  f.PreCreditPeriodType_EffectFromDate,
  f.PreCreditPeriodType_EffectToDate,
  f.PreCreditPeriodType_PorceCode,
  f.PreCreditPeriodTypePormeCode,
  f.MaxCrd_Typ as Max_PCR_Period_Type,
  f.MaxCrd_Value as Max_PCR_Period,
  f.MaxCrd_PerfDat,
  f.SelPCRCover as Selective_PCR_cover,
  case
    when a.policyid = g.policyid
    and a.CoverStartDate = g.CoverStartDate
    and a.CoverEndDate = a.RiskPeriodEndDate then True
    Else False
  End as LastContractCohort,
  h.RunoffCoverPorceCode,
  h.RunoffCoverPormeCode,
  h.RunoffCoverPorveCode,
  h.RunoffCoverValue,
  h.RunoffCoverLastUpdateDat,
  i.d_popss_status_code as Latest_Status_Code,
  g.start_risk_dat as PolicyStartDate,
  --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate
  g.end_risk_dat as PolicyEndDate -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate
From
  $Databases.Contracts a
  left join $Databases.TBPO_POL_MODULES mod on mod.Bupiy_Id = a.PolicyId
  Left Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES refmodvar On (
    mod.porce_code = refmodvar.porce_code
    and mod.porme_Code = refmodvar.porme_code
  )
  Left Join $Databases.TBPO_POL_MOD_VARIABLES modvar On (
    refmodvar.porve_Code = modvar.porve_code
    and mod.bupiy_id = modVar.Bupiy_id
  ) --Drivers
   --MaximumCreditTerms
  left join MaximumCreditTerms b on (
    a.policyid = b.Bupiy_Id
    and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') between to_date(b.effect_from_dat, 'yyyyMMdd')
    and to_date(b.effect_to_dat, 'yyyyMMdd')
    and to_date(cast(a.contractissuedate as string), 'yyyyMMdd') = to_date(b.Start_Risk_Dat, 'yyyyMMdd')
  ) --MaximumCreditPeriod
  left join MaximumCreditPeriod c on (
    a.policyid = c.PolicyId
    and to_date(c.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  ) --DeviatingCreditTerms
  Left Join (
    SELECT
      pol.bupiy_id as PolicyId,
      Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
      ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
    FROM
      $Databases.TBPO_POL_VERSIONS pol
      JOIN $Databases.TBPO_CTRY_GRP_MCTS ctrygrp on (
        ctrygrp.bupiy_id = pol.bupiy_id
        AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
        AND ctrygrp.effect_to_dat
      )
    where
      ctrygrp.max_credit_terms2_per is not null
      and ctrygrp.max_credit_terms2_per_typ is not null
    GROUP BY
      pol.bupiy_id,
      ctrygrp.max_credit_terms2_per_typ
  ) d on a.PolicyId = d.PolicyId --WaitingPeriod
  Left Join (
    SELECT
      pol.bupiy_id as PolicyId,
      max(ctrygrpcvr.waiting_period) as WaitingPeriod,
      ctrygrpcvr.waiting_period_typ as WaitingPeriodType
    FROM
      $Databases.TBPO_POL_VERSIONS pol
      JOIN $Databases.TBPO_CTRY_GRP_COVER_PCTS ctrygrpcvr on (
        ctrygrpcvr.bupiy_id = pol.bupiy_id
        AND pol.end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat
        AND ctrygrpcvr.effect_to_dat
      )
    Where
      Waiting_period <> 0
    GROUP BY
      pol.Bupiy_id,
      ctrygrpcvr.waiting_period_typ
  ) e on a.PolicyId = e.PolicyId --PreCreditPeriodType
  left join (
    select
      *
    from
      (
        Select
          pmod.Bupiy_Id as PolicyId,
          pmod.Effect_from_dat as PreCreditPeriodType_EffectFromDate,
          pmod.Effect_to_Dat as PreCreditPeriodType_EffectToDate,
          pmod.Porce_code as PreCreditPeriodType_PorceCode,
          pmod.porme_code as PreCreditPeriodTypePormeCode,
          mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
          --mvar.EFFECT_FROM_DAT, -->redundanet
          --mvar.Effect_to_dat, -->redundant
          mvar.value as PreCreditPeriodTypeValue
        From
          $Databases.TBBU_POLICIES a
          left join $Databases.TBPO_POL_MODULES pmod on pmod.bupiy_id = a.id
          Left Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES mref On pmod.porme_code = mref.porme_code
          And pmod.porce_code = mref.Porce_code
          Left Join $Databases.TBPO_POL_MOD_VARIABLES mvar On pmod.Bupiy_Id = mvar.Bupiy_Id
          And mref.Porve_code = mvar.porve_Code
        Where
          a.d_popvn_end_risk_dat between pmod.effect_from_dat
          and pmod.effect_to_dat
          And a.d_popvn_end_risk_dat between mvar.effect_from_dat
          and mvar.effect_to_dat
          And (
            pmod.PORME_CODE like ('43500.%')
            OR pmod.PORME_CODE like ('43800.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44405.%')
            OR pmod.PORME_CODE like ('44400.%')
            OR pmod.PORME_CODE like ('44410.%')
          )
      ) pivot (
        max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in (
          'PERTYP3' as MaxCrd_Typ,
          'MAXPRCR' as MaxCrd_Value,
          'PERFDAT' as MaxCrd_PerfDat,
          'SELPCRCO' as SelPCRCover
        )
      )
  ) f on a.PolicyId = f.PolicyId 
  
  --LastContractCohort
  left join (
    select
      policyid,
      max(CoverStartDate) as CoverStartDate
    from
      $Databases.Contracts
    where
      CoverEndDate = RiskPeriodEndDate
      and datasource = 'SYM'
    group by
      policyid
  ) g on a.policyid = g.policyid 
  
  --RunoffCover
  left join RunoffCover h on (
    a.policyid = h.PolicyId
    and to_date(h.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  ) --NoCoverPeriod
  Left Join $Databases.TBBU_POLICIES i on a.policyid = i.id
  left join $Databases.TBPO_POL_VERSIONS g on (
    a.policyid = g.bupiy_id
    and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  )
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('LOFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat
  And a.RiskPeriodEndDate>a.ValuationDate
  And a.DataSource = 'SYM'

-- COMMAND ----------

-- MAGIC %md #Control2

-- COMMAND ----------

Create view $Databases.Control2 as
Select
distinct
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.CMPCT_ID,
  b.POLICY_STATUS,
  b.BREAK_MODULE_CODE,
  b.EFFECTIVE_FROM,
  b.EFFECTIVE_TO,
  d.start_risk_dat as PolicyStartDate, --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate  ---> Amir request
  d.end_risk_dat as PolicyEndDate, -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate    ---> Amir request
  c.unicode_value as Variable_Value,
  d.MaximumCreditPeriod,
  d.MaximumCreditPeriodTermsType,
  f.PreCreditPeriodType_EffectFromDate,
  f.PreCreditPeriodType_EffectToDate,
  f.PreCreditPeriodType_PorceCode,
  f.PreCreditPeriodTypePormeCode,
  f.MaxCrd_Typ as Max_PCR_Period_Type,
  f.MaxCrd_Value as Max_PCR_Period,
  f.MaxCrd_PerfDat,
  f.SelPCRCover as Selective_PCR_cover,
  e.WaitingPeriod,
  e.WaitingPeriodType
from
  (select
  policyid,
  ContractIssueDate,
  count(contractid)
from
  $Databases.Contracts
where 
  DataSource='SYM'
  and RiskPeriodEndDate>ValuationDate
group by
  policyid,
  ContractIssueDate
having
  count(contractid) > 12) b
  left join $Databases.Contracts a on (b.PolicyId=a.PolicyId and b.ContractIssueDate=a.ContractIssueDate)
  left join (
    SELECT
      a.ID POLICY_ID,
      a.CMPCT_ID CMPCT_ID,
      a.d_popss_status_code POLICY_STATUS,
      b.PORME_CODE BREAK_MODULE_CODE,
      b.EFFECT_FROM_DAT EFFECTIVE_FROM,
      b.EFFECT_TO_DAT EFFECTIVE_TO
    FROM
      $Databases.TBBU_POLICIES a
      left join $Databases.TBPO_POL_MODULES b on a.ID = b.BUPIY_ID --AND d_popss_status_code in ('LIVE', 'SUSP')
      AND (
        b.PORME_CODE LIKE '39400.__'
        OR b.PORME_CODE LIKE '62102.__'
        OR b.PORME_CODE LIKE '62239.__'
        OR b.PORME_CODE LIKE '62045.__'
        OR b.PORME_CODE LIKE '62295.__'
        OR b.PORME_CODE LIKE '62405.__'
        OR b.PORME_CODE LIKE '62485.__'
        OR b.PORME_CODE LIKE '62499.__'
        OR b.PORME_CODE LIKE '62507.__'
        OR b.PORME_CODE LIKE '62562.__'
        OR b.PORME_CODE LIKE '62721.__'
        OR b.PORME_CODE LIKE '77003.__'
        OR b.PORME_CODE LIKE '39360.__'
        OR b.PORME_CODE LIKE '39370.__'
      )
      and b.Effect_To_Dat > Current_Date()
  ) b on a.PolicyId = b.POLICY_ID
  left join $Databases.TBPO_POL_VERSIONS d on (a.policyid = d.bupiy_id and to_date(d.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
  left join  $Databases.TBPO_POL_MOD_VARIABLES c on (a.policyid=c.bupiy_id  and Current_Date() between c.effect_from_dat and c.effect_to_dat and c.porve_code IN ('RAFLG', 'LOFLG')) 
   --MaximumCreditPeriod  Based on Uli request on 1 April 2022
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
            ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
            FROM $Databases.TBPO_POL_VERSIONS pol
            JOIN $Databases.TBPO_CTRY_GRP_MCTS ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_cred_per_typ) d on a.PolicyId=d.PolicyId
    --WaitingPeriod Based on Uli request on 1 April 2022
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            max(ctrygrpcvr.waiting_period) as WaitingPeriod,
            ctrygrpcvr.waiting_period_typ as WaitingPeriodType
            FROM
            $Databases.TBPO_POL_VERSIONS pol
            JOIN $Databases.TBPO_CTRY_GRP_COVER_PCTS ctrygrpcvr on (
            ctrygrpcvr.bupiy_id = pol.bupiy_id
            AND pol.end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat AND ctrygrpcvr.effect_to_dat )
            Where
            Waiting_period <> 0
            GROUP BY
            pol.Bupiy_id, ctrygrpcvr.waiting_period_typ) e on a.PolicyId=e.PolicyId
    --PreCreditPeriodType Based on Uli request on 1 April 2022
  left join (select * from (Select
            pmod.Bupiy_Id as PolicyId,
            pmod.Effect_from_dat as PreCreditPeriodType_EffectFromDate,
            pmod.Effect_to_Dat as PreCreditPeriodType_EffectToDate,
            pmod.Porce_code as PreCreditPeriodType_PorceCode,
            pmod.porme_code as PreCreditPeriodTypePormeCode,
            mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
            --mvar.EFFECT_FROM_DAT, -->redundanet
            --mvar.Effect_to_dat, -->redundant
            mvar.value as PreCreditPeriodTypeValue
            From $Databases.TBBU_POLICIES a
            left join $Databases.TBPO_POL_MODULES pmod on pmod.bupiy_id=a.id
            Left  Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES mref
            On pmod.porme_code = mref.porme_code
            And pmod.porce_code = mref.Porce_code
            Left  Join $Databases.TBPO_POL_MOD_VARIABLES mvar
            On pmod.Bupiy_Id = mvar.Bupiy_Id
            And mref.Porve_code = mvar.porve_Code
            Where a.d_popvn_end_risk_dat between pmod.effect_from_dat and pmod.effect_to_dat
            And a.d_popvn_end_risk_dat between mvar.effect_from_dat and mvar.effect_to_dat
            And
            ( pmod.PORME_CODE like ('43500.%')
            OR pmod.PORME_CODE like ('43800.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44100.%')
            OR pmod.PORME_CODE like ('44405.%')
            OR pmod.PORME_CODE like ('44400.%')
            OR pmod.PORME_CODE like ('44410.%')
            ))
            pivot (max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in 
            ('PERTYP3' as MaxCrd_Typ ,'MAXPRCR' as MaxCrd_Value,'PERFDAT' as MaxCrd_PerfDat,'SELPCRCO' as SelPCRCover))) f on a.PolicyId=f.PolicyId
where
  datasource = 'SYM'
  and a.RiskPeriodEndDate>a.ValuationDate



-- COMMAND ----------

-- MAGIC %md #Control3

-- COMMAND ----------

Create view $Databases.Control3 as
select
  *
from
  (
    Select
      a.*,
      Left(a.ContractIssueDate, 4) as CohortYear,
      case
        when cancellability = 0 then True
        Else False
      End as cancellabilityFlag,
      pmod.Effect_from_dat,
      pmod.Effect_to_Dat,
      pmod.Porce_code as Component_Code,
      pmod.porme_code as Module_Code,
      mvar.PORVE_CODE,
      mvar.value
    From
      $Databases.Contracts a
      left join $Databases.TBPO_POL_MODULES pmod on a.PolicyId = pmod.Bupiy_Id
      Left Outer Join $Databases.TBPO_REF_MODULE_VARIABLE_TYPES mref On (
        pmod.porme_code = mref.porme_code
        And pmod.porce_code = mref.Porce_code
      )
      Left Outer Join $Databases.TBPO_POL_MOD_VARIABLES mvar On (
        pmod.Bupiy_Id = mvar.Bupiy_Id
        And mref.Porve_code = mvar.porve_Code
      )
    Where
      Current_date() between pmod.effect_from_dat
      and pmod.effect_to_dat
      And Current_date() between mvar.effect_from_dat
      and mvar.effect_to_dat
      And (
        pmod.PORME_CODE like ('15200.%')
        OR pmod.PORME_CODE like ('15206.%')
        OR pmod.PORME_CODE like ('00105.%') -- MBNCIND = MBNC Policy Indicator
      )
      And a.RiskPeriodEndDate>a.ValuationDate
      And a.DataSource = 'SYM'
      and cancellability != 0
  ) pivot (
    max(value) for porve_code in (
      'NROFDAYS' as Withrawal_Days,
      'NROFMONT' as Withdawal_Month
    )
  )

-- COMMAND ----------

-- MAGIC %md #Control5

-- COMMAND ----------

create view $Databases.Control5 as
select
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id,
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether,
  b.d_popss_status_code,
  d.master_flag -- requested by Uli om 2 June 2022
from
  $Databases.Contracts a
  left join $Databases.TBBU_POLICIES b on a.PolicyId = b.id
  Left join $Databases.TBOR_NON_NCM_ORGANISATIONS c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join $Databases.TBPO_POL_GROUP_POLICIES d on (
    a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd')
    and to_date(d.effect_to_dat, 'yyyyMMdd')
  ) --ContractIssueDate has been used to match between effect from and to date
where
  datasource = 'SYM'
  and a.RiskPEriodEndDate>ValuationDate

-- COMMAND ----------

-- MAGIC %md #Control9

-- COMMAND ----------

create view $Databases.Control9 as
with Control9Int (
select * from $Databases.Contracts where ManagedTogetherId in (select
  ManagedTogetherId
from
  $Databases.Contracts
where
  DataSource='SYM' and RiskPeriodEnDate>ValuationDate
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
except
select
  ManagedTogetherId
from
  $Databases.Contracts
where
  mainunit like 'GLB%') and DataSource='SYM'
)
select
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id, -- Based on Amir req
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether
from
  Control9Int a
  left join $Databases.TBBU_POLICIES b on a.PolicyId = b.id
  Left join $Databases.TBOR_NON_NCM_ORGANISATIONS c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join $Databases.TBPO_POL_GROUP_POLICIES d on ( a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd') and to_date(d.effect_to_dat, 'yyyyMMdd'))  --ContractIssueDate has been used to match between effect from and to date
