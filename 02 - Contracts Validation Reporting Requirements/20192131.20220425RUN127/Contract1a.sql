-- Databricks notebook source
show tables in MI2022

control1a_20191231_20220425_run127
control1b_20191231_20220425_run127
control2_20191231_20220425_run127
control3_20191231_20220425_run127
control5_20191231_20220425_run127
control9_20191231_20220425_run127

-- COMMAND ----------

drop view MI2022.Control1a_20191231_20220425_RUN127 

-- COMMAND ----------

Create view MI2022.Control1a_20191231_20220425_RUN127 as 
with MaximumCreditTerms as (
  select
    pol.Bupiy_id,
    start_risk_dat,
    Max(ctrygrp.max_credit_terms_per) as MaximumCreditTerms,
    ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
  from
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
    Inner JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (
      ctrygrp.bupiy_id = pol.Bupiy_id
      AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
      AND ctrygrp.effect_to_dat
    )
  group by
    pol.Bupiy_id,
    start_risk_dat,
    ctrygrp.max_credit_terms_per_typ
),

MaximumCreditPeriod as (
  SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType,
    pol.start_risk_dat
  FROM
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (
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
  distinct
  a.ValuationDate,
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
  ) as CoveragePeriodDaysDiff,   --step1
  months_between(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) as CoveragePeriodMontDiff,   --step1
  DATEDIFF(
    to_date(cast(a.CoverEndDate as string), 'yyyyMMdd'),
    to_date(cast(a.CoverStartDate as string), 'yyyyMMdd')
  ) / 360 as CoveragePeriodYearsDiff,  --step1
  to_date(cast(a.BoundDate as string), 'yyyyMMdd') as BoundDate,
  to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') as ContractIssueDate,
  Left(a.ContractIssueDate, 4) as CohortYear,
  to_date(cast(a.ContractInceptionDate as string), 'yyyyMMdd') as ContractInceptionDate,
  to_date(cast(a.RiskPeriodStartDate as string), 'yyyyMMdd') as RiskPeriodStartDate,
  to_date(cast(a.RiskPeriodEndDate as string), 'yyyyMMdd') as RiskPeriodEndDate,
  a.Cancellability,
  a.InitialProfitabilityClassing,
  a.ProductType,
  a.MainProduct,
  a.Unit,
  a.MainUnit,
  mod.porce_Code as Component_Code,  --step0
  mod.porme_code as Module_Code,  --step0
  refmodvar.porve_code as Variable_Code, --step0
  b.MaximumCreditTerms,
  b.MaximumCreditTermsType,
  c.MaximumCreditPeriod,
  c.MaximumCreditPeriodTermsType,
  d.DeviatingMaximumCreditTerms,
  d.DeviatingMaximumCreditTermsType,
  e.WaitingPeriod,
  e.WaitingPeriodType,
  Case 
  When i.d_popvn_start_risk_dat = i.d_popvn_end_risk_dat then True
  Else False
  End as NoCoverPeriod,
  modvar.effect_from_dat, --step0
  modvar.effect_to_dat, --step0
  modvar.value, --step0
  modvar.orsus_id as UserId_Last_Update, --step0
  modvar.last_update_dat, --step0
  modvar.orsus_create_id as UserId_Created,
  modvar.create_dat, --step0
  modvar.change_dat, --step0
  modvar.seq as Sequence, --step0
  modvar.origin_ind, --step0
  modvar.unicode_value as Variable_Value, --step0
  f.PreCreditPeriodType_EffectFromDate,
  f.PreCreditPeriodType_EffectToDate,
  f.PreCreditPeriodType_PorceCode,
  f.PreCreditPeriodTypePormeCode,
  f.MaxCrd_Typ as Max_PCR_Period_Type,
  f.MaxCrd_Value as Max_PCR_Period,
  f.MaxCrd_PerfDat,
  f.SelPCRCover as Selective_PCR_cover,
  i.d_popss_status_code as Latest_Status_Code,
  g.start_risk_dat as PolicyStartDate, --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate
  g.end_risk_dat as PolicyEndDate -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate
From
  MI2022.Contracts_SL1_20191231_20220425_RUN127 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_20220425_RUN127 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_20220425_RUN127 refmodvar On (mod.porce_code = refmodvar.porce_code and mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_20220425_RUN127 modvar On (refmodvar.porve_Code = modvar.porve_code and mod.bupiy_id = modVar.Bupiy_id)
  --Drivers
  --MaximumCreditTerms
   left join MaximumCreditTerms b on (a.policyid = b.Bupiy_id
  and to_date(start_risk_dat, 'yyyyMMdd') = to_date(cast(ContractIssueDate as string), 'yyyyMMdd'))
  --MaximumCreditPeriod
  left join MaximumCreditPeriod c on ( a.policyid = c.PolicyId
    and to_date(c.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd'))
  --DeviatingCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where
            ctrygrp.max_credit_terms2_per is not null
            and ctrygrp.max_credit_terms2_per_typ is not null
            GROUP BY
            pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ) d on a.PolicyId=d.PolicyId
  --WaitingPeriod
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            max(ctrygrpcvr.waiting_period) as WaitingPeriod,
            ctrygrpcvr.waiting_period_typ as WaitingPeriodType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_20220425_RUN127 ctrygrpcvr on (
            ctrygrpcvr.bupiy_id = pol.bupiy_id
            AND pol.end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat AND ctrygrpcvr.effect_to_dat )
            Where
            Waiting_period <> 0
            GROUP BY
            pol.Bupiy_id, ctrygrpcvr.waiting_period_typ) e on a.PolicyId=e.PolicyId
  --PreCreditPeriodType
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
            From MI2022.TBBU_POLICIES_SL1_20191231_20220425_RUN127 a
            left join MI2022.TBPO_POL_MODULES_SL1_20191231_20220425_RUN127 pmod on pmod.bupiy_id=a.id
            Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_20220425_RUN127 mref
            On pmod.porme_code = mref.porme_code
            And pmod.porce_code = mref.Porce_code
            Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_20220425_RUN127 mvar
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
  --NoCoverPeriod
  Left Join MI2022.TBBU_POLICIES_SL1_20191231_20220425_RUN127 i on a.policyid=i.id
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 g on (a.policyid = g.bupiy_id and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached 
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat and modvar.effect_to_dat 
  And a.CoverEndDate > 20191231
  And a.DataSource = 'SYM'

-- COMMAND ----------

select count(*) from MI2022.Control1a_20191231_20220425_RUN127

-- COMMAND ----------

select count(*) from MI2022.Control1a_20201231_v20220419_01

-- COMMAND ----------

select * from  MI2022.Contracts_SL1_20191231_20220425_RUN127 a
             left join MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 b on ( a.policyid=b.bupiy_id and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd'))=to_date(b.effect_from_dat, 'yyyyMMdd') and         to_date(b.effect_to_dat, 'yyyyMMdd')

-- COMMAND ----------

select
  *
from
  (
    SELECT
    a.*,
      bupiy_id,
      max_credit_terms2_per as DeviatingMaximumCreditTerms,
      max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType,
      effect_from_dat,
      effect_to_dat
    FROM
      MI2022.Contracts_SL1_20191231_20220425_RUN127 a
      left join MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 b on (
        a.policyid = b.bupiy_id
        and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
       between to_date(b.effect_from_dat, 'yyyyMMdd')
      and to_date(b.effect_to_dat, 'yyyyMMdd'))
  
)
where
  bupiy_id = 6135 and coverenddate>20191231

-- COMMAND ----------

select * from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 where bupiy_id=6135

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127

-- COMMAND ----------

select * from MI2022.contracts_SL1_20191231_20220425_RUN127 where PolicyId=6135 and coverenddate>20191231

-- COMMAND ----------

select * from 
MI2022.Contracts_SL1_20191231_20220425_RUN127 a left join
(SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            GROUP BY
            pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ)b on 
            (a.policyid = b.PolicyId
        and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
       between to_date(b.effect_from_dat, 'yyyyMMdd')
      and to_date(b.effect_to_dat, 'yyyyMMdd'))
            where PolicyId=6135 and coverenddate>20191231

-- COMMAND ----------

select * from 
(SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType,
            ctrygrp.effect_from_dat,
            ctrygrp.effect_to_dat
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            
            GROUP BY
            pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ,ctrygrp.effect_from_dat,
            ctrygrp.effect_to_dat) where PolicyId=6135

-- COMMAND ----------

select * from 
MI2022.Contracts_SL1_20191231_20220425_RUN127 a left join
(SELECT
            pol.bupiy_id ,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType,
            ctrygrp.effect_from_dat,
            ctrygrp.effect_to_dat
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            
            GROUP BY
            pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ,ctrygrp.effect_from_dat,
            ctrygrp.effect_to_dat) b on 
            (a.policyid = b.bupiy_id
        and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd')
       between to_date(b.effect_from_dat, 'yyyyMMdd')
      and to_date(b.effect_to_dat, 'yyyyMMdd'))
            where PolicyId=6135 and coverenddate>20191231

-- COMMAND ----------

select * from 
(SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            GROUP BY
            pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ) where PolicyId=6135

-- COMMAND ----------

Select
  ctrygrp.Bupiy_Id,
  MAx(Effect_from_Dat),
  MAx(Effect_To_Dat),
  MAX(MAX_Credit_Terms_PER),
  MAX(MAx_Credit_terms_per_typ),
  MAX(MAX_CRedit_terms2_per),
  MAx(MAX_CREDIT_TERMS2_PER_TYP)
From
  MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pol left join
  MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
where
  ctrygrp.bupiy_id = 6135
Group By
  ctrygrp.Bupiy_Id

-- COMMAND ----------

Select Bupiy_Id, MAx(Effect_from_Dat), MAx(Effect_To_Dat), MAX(MAX_Credit_Terms_PER), MAX(MAx_Credit_terms_per_typ), MAX(MAX_CRedit_terms2_per), MAx(MAX_CREDIT_TERMS2_PER_TYP)
From MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 where bupiy_id = 6135
and to_Date('30.09.2020','dd.mm.yyyy')between effect_from_DAT and effect_To_Dat
Group By Bupiy_Id

-- COMMAND ----------

select
  *
from
  MI2022.Contracts_SL1_20191231_20220425_RUN127 a
  left join (
    Select
      pvs.Bupiy_Id,
      pvs.Start_Risk_Dat,
      pvs.End_Risk_Dat,
      MAx(Effect_from_Dat) as effect_from_dat,
      MAx(Effect_To_Dat) as effect_to_dat,
      MAX(MAX_Credit_Terms_PER),
      MAX(MAx_Credit_terms_per_typ),
      MAX(MAX_CRedit_terms2_per),
      MAx(MAX_CREDIT_TERMS2_PER_TYP)
    From
      MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pvs
      Left Join MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctry On pvs.bupiy_id = ctry.Bupiy_id
      And pvs.end_risk_dat between ctry.effect_from_DAT
      and ctry.effect_To_Dat
    where
      pvs.bupiy_id = 6135
    Group By
      pvs.Bupiy_Id,
      pvs.Start_Risk_Dat,
      pvs.End_Risk_Dat
    ORDER BY
      2 ASC NULLS LAST
  ) b on (
    a.policyid = b.Bupiy_Id
    and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') between to_date(b.effect_from_dat, 'yyyyMMdd')
    and to_date(b.effect_to_dat, 'yyyyMMdd')
  )
where
  policyid = 6135
  and coverenddate > 20191231

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_20220425_RUN127 where policyid=6135

-- COMMAND ----------

max_credit_terms_per
max_credit_terms_per_typ
max_credit_terms2_per
max_credit_terms2_per_typ

select * from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 where bupiy_id=6135

-- COMMAND ----------

select
  *
from
  
 MI2022.Contracts_SL1_20191231_20220425_RUN127 a 
Left join ( select
  
    distinct bupiy_id,effect_to_dat,
    max_credit_terms_per,
    max_credit_terms_per_typ,
    COALESCE(max_credit_terms2_per, 0),
    COALESCE(max_credit_terms2_per_typ, 0)
    from
      MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127
  ) b
  on  a.policyid=b.bupiy_id
where
  bupiy_id = 6135
  and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') <
   to_date(b.effect_to_dat, 'yyyyMMdd')

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 where bupiy_id=6135

-- COMMAND ----------

-- The new implemntation

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_20220425_RUN127 a left join

(Select pvs.Bupiy_Id, pvs.Start_Risk_Dat, pvs.End_Risk_Dat, MAx(Effect_from_Dat) as Effect_from_Dat, MAx(Effect_To_Dat) as Effect_To_Dat, MAX(MAX_Credit_Terms_PER), MAX(MAx_Credit_terms_per_typ), MAX(MAX_CRedit_terms2_per),  MAx(MAX_CREDIT_TERMS2_PER_TYP)
From MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pvs
Left Join MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctry
On pvs.bupiy_id = ctry.Bupiy_id
And pvs.end_risk_dat between ctry.effect_from_DAT and ctry.effect_To_Dat
Group By pvs.Bupiy_Id, pvs.Start_Risk_Dat, pvs.End_Risk_Dat
ORDER BY 2 ASC NULLS LAST) b on (a.policyid=b.Bupiy_Id and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') between to_date(b.effect_from_dat, 'yyyyMMdd')
    and to_date(b.effect_to_dat, 'yyyyMMdd') and to_date(cast(a.contractissuedate as string), 'yyyyMMdd')=to_date(b.Start_Risk_Dat, 'yyyyMMdd'))


where policyid = 6135

-- COMMAND ----------

DeviatingMaximumCreditTerms

DeviatingMaximumCreditTermsType

-- COMMAND ----------

select a.contractid,count(*) from (

select * from MI2022.Contracts_SL1_20191231_20220425_RUN127 a left join

(Select pvs.Bupiy_Id, pvs.Start_Risk_Dat, pvs.End_Risk_Dat, MAx(Effect_from_Dat) as Effect_from_Dat, MAx(Effect_To_Dat) as Effect_To_Dat, MAX(MAX_Credit_Terms_PER), MAX(MAx_Credit_terms_per_typ), MAX(MAX_CRedit_terms2_per),  MAx(MAX_CREDIT_TERMS2_PER_TYP)
From MI2022.TBPO_POL_VERSIONS_SL1_20191231_20220425_RUN127 pvs
Left Join MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_20220425_RUN127 ctry
On pvs.bupiy_id = ctry.Bupiy_id
And pvs.end_risk_dat between ctry.effect_from_DAT and ctry.effect_To_Dat
Group By pvs.Bupiy_Id, pvs.Start_Risk_Dat, pvs.End_Risk_Dat
ORDER BY 2 ASC NULLS LAST) b on (a.policyid=b.Bupiy_Id and to_date(cast(a.RiskPEriodEndDate as string), 'yyyyMMdd') between to_date(b.effect_from_dat, 'yyyyMMdd')
    and to_date(b.effect_to_dat, 'yyyyMMdd') and to_date(cast(a.contractissuedate as string), 'yyyyMMdd')=to_date(b.Start_Risk_Dat, 'yyyyMMdd'))) a group by contractid having count(*)>1 order by 2 desc


-- COMMAND ----------

select a.contractid,count(*) from MI2022.Contracts_SL1_20191231_20220425_RUN127  a group by contractid having count(*)>1 order by 2 desc


-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_20220425_RUN127 where contractid='SYM / 1131879 / 20220101'
