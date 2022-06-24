-- Databricks notebook source
-- MAGIC %md #Step 0: Select risk attaching policies

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=1000817

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where RiskPeriodEndDate > 20191231 And DataSource = 'SYM'

-- COMMAND ----------

select count(*) from (
Select
  a.*,
  mod.porce_Code,
  mod.porme_code,
  refmodvar.porve_code,
  modvar.*
From
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code
  And mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code
  And mod.bupiy_id = modVar.Bupiy_id)
Where
   mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'
  )

-- COMMAND ----------

Select
  a.*,
  mod.porce_Code,
  mod.porme_code,
  refmodvar.porve_code,
  modvar.*
From
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code
  And mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code
  And mod.bupiy_id = modVar.Bupiy_id)
Where
   mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'

-- COMMAND ----------

-- MAGIC %md #Step 1: Off each policy select the first contract and calculate the length of coverageperiod (in number of months/days) and order the policies from short period to long period

-- COMMAND ----------

select policyid,contractid,ContractIssueDate, 
to_date(cast(CoverEndDate as string),'yyyyMMdd') as CoverEndDate,
to_date(cast(CoverStartDate as string),'yyyyMMdd') as CoverStartDate,
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as DaysDiff,
months_between(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as MontDiff, 
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') )/360 as YearsDiff,a.*
from MI2022.Contracts_SL1_20191231_v20220228_01 a where CoverStartDate=ContractIssueDate and DataSource='SYM'
order by 6 desc

-- COMMAND ----------

-- MAGIC %md #Step 3: To the list add contract features which are drivers behind the coverenddate: like(Maximal Credit Term, MEP, Waiting Period, Binding Cover, PCR, Runoff cover) as all these have impact on CoverEndDate

-- COMMAND ----------

-- MAGIC %md ###MaximumCreditTerms

-- COMMAND ----------

SELECT
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
GROUP BY
pol.bupiy_id
,ctrygrp.max_credit_terms_per_typ


-- COMMAND ----------

select policyid,count(*) from (
SELECT
distinct
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
FROM 
MI2022.Contracts_SL1_20191231_v20220228_01 a
Left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.policyid=pol.bupiy_id
JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
where a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM' and ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
GROUP BY
pol.bupiy_id
,ctrygrp.max_credit_terms_per_typ) group by policyid having count(*)>1 order by 2 desc

-- COMMAND ----------

-- MAGIC %md ###MaximumCreditPeriod

-- COMMAND ----------

SELECT
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
ctrygrp.max_cred_per_typ as MaximumCreditTermsType
FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
AND pol .end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
GROUP BY
pol.bupiy_id
,ctrygrp.max_cred_per_typ

-- COMMAND ----------

select policyid,count(*) from (
SELECT
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
ctrygrp.max_cred_per_typ as MaximumCreditTermsType
FROM  MI2022.Contracts_SL1_20191231_v20220228_01 a
left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on pol.bupiy_id =a.PolicyId
JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
AND pol .end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
where a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM' and ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
GROUP BY
pol.bupiy_id
,ctrygrp.max_cred_per_typ) group by policyid having count(*)>1 order by 2 desc

-- COMMAND ----------

-- MAGIC %md ###DeviatingCreditTerms

-- COMMAND ----------

SELECT
  pol.bupiy_id as PolicyId,
  Max(ctrygrp.max_credit_terms2_per) as maximumCreditTerms,
  ctrygrp.max_credit_terms2_per_typ as MaximumCreditTermsType
FROM
  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
  JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id = pol.bupiy_id
  AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
  AND ctrygrp.effect_to_dat
where
  ctrygrp.max_credit_terms2_per is not null
  and ctrygrp.max_credit_terms2_per_typ is not null
GROUP BY
  pol.bupiy_id,
  ctrygrp.max_credit_terms2_per_typ

-- COMMAND ----------

select policyid,count(*) from (
SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms2_per) as maximumCreditTerms,
    ctrygrp.max_credit_terms2_per_typ as MaximumCreditTermsType
FROM MI2022.Contracts_SL1_20191231_v20220228_01 a
left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on pol.bupiy_id =a.PolicyId
JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
AND pol .end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
where a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM' and ctrygrp.max_credit_terms2_per is not null and ctrygrp.max_credit_terms2_per_typ is not null
GROUP BY
pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ ) group by policyid having count(*)>1 order by 2 desc


-- COMMAND ----------

-- MAGIC %md ###WaitingPeriod

-- COMMAND ----------

SELECT
  pol.bupiy_id as PolicyId,
  max(ctrygrpcvr.waiting_period) as WaitingPeriod,
  ctrygrpcvr.waiting_period_typ as WaitingPeriodType
FROM
  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
  JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrpcvr on (
    ctrygrpcvr.bupiy_id = pol.bupiy_id
    AND pol.end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat
    AND ctrygrpcvr.effect_to_dat
  )
Where
  Waiting_period <> 0
GROUP BY
  pol.Bupiy_id,
  ctrygrpcvr.waiting_period_typ

-- COMMAND ----------

select policyid,count(*) from (
SELECT
    pol.bupiy_id as PolicyId,
    max(ctrygrpcvr.waiting_period) as WaitingPeriod,
    ctrygrpcvr.waiting_period_typ as WaitingPeriodType
FROM MI2022.Contracts_SL1_20191231_v20220228_01 a
left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on pol.bupiy_id =a.PolicyId
JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrpcvr on (ctrygrpcvr.bupiy_id=pol.bupiy_id
AND pol .end_risk_dat BETWEEN ctrygrpcvr.effect_from_dat AND ctrygrpcvr.effect_to_dat)
Where Waiting_period <>0 and a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'
 GROUP BY
pol.Bupiy_id,ctrygrpcvr.waiting_period_typ) group by policyid having count(*)>1 order by 2 desc


-- COMMAND ----------

-- MAGIC %md ###PreCreditPeriodType 

-- COMMAND ----------

Select
  pvar.bupiy_Id as PolicyId,
  refmodvar.porce_code,
  refmodvar.porme_code,
  pvar.porve_Code,
  pvar.Value,
  pvar.last_update_dat
From
  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On pvar.porve_code = refmodvar.porve_code
  left join  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on pol.bupiy_Id=pvar.bupiy_Id
Where
pvar.bupiy_Id=654571 and
  pvar.porve_code in (
    'MAXPRCR',
    'PERFDAT',
    'PERTYP3',
    'PERTYP',
    'MAXEXPER',
    'POLPER'
  )
  And pol.end_risk_dat between pvar.effect_from_dat and pvar.effect_to_dat
Group By
  pvar.bupiy_Id,
  refmodvar.porce_code,
  refmodvar.porme_code,
  pvar.porve_Code,
  pvar.Value,
  pvar.last_update_dat
Having
  pvar.last_update_dat = max(pvar.last_update_dat)

-- COMMAND ----------

-- MAGIC %md The new code provided by Uli

-- COMMAND ----------

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
From MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod on pmod.bupiy_id=a.id
Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
On pmod.porme_code = mref.porme_code
And pmod.porce_code = mref.Porce_code
Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
On pmod.Bupiy_Id = mvar.Bupiy_Id
And mref.Porve_code = mvar.porve_Code
Where pmod.Bupiy_id = 654571
And a.d_popvn_end_risk_dat between pmod.effect_from_dat and pmod.effect_to_dat
And a.d_popvn_end_risk_dat between mvar.effect_from_dat and mvar.effect_to_dat
And
( pmod.PORME_CODE like ('43500.%')
OR pmod.PORME_CODE like ('43800.%')
OR pmod.PORME_CODE like ('44100.%')
OR pmod.PORME_CODE like ('44100.%')
OR pmod.PORME_CODE like ('44405.%')
OR pmod.PORME_CODE like ('44400.%')
OR pmod.PORME_CODE like ('44410.%')
)

-- COMMAND ----------

-- MAGIC %md after pivoting

-- COMMAND ----------

select * from (Select
pmod.Bupiy_Id as PolicyId,
pmod.Effect_from_dat as PreCreditPeriodType_EffectFromDate,
pmod.Effect_to_Dat as PreCreditPeriodType_EffectToDate,
pmod.Porce_code as PreCreditPeriodType_PorceCode,
pmod.porme_code as PreCreditPeriodTypePormeCode,
mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
--mvar.EFFECT_FROM_DAT, -->redundanet
--mvar.Effect_to_dat, -->redundant
mvar.value as PreCreditPeriodTypeValue
From MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod on pmod.bupiy_id=a.id
Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
On pmod.porme_code = mref.porme_code
And pmod.porce_code = mref.Porce_code
Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
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
pivot (max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in ('PERTYP3' as MaxCrd_Typ ,'MAXPRCR' as MaxCrd_Value,'PERFDAT' as MaxCrd_PerfDat,'SELPCRCO' as SelPCRCover))

-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

drop view MI2022.Control1a 

-- COMMAND ----------

Create view MI2022.Control1a as 
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
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code and mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code and mod.bupiy_id = modVar.Bupiy_id)
  --Drivers
  --MaximumCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
            ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
            AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId
  --MaximumCreditPeriod
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
            ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_cred_per_typ) c on a.PolicyId=c.PolicyId
  --DeviatingCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
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
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrpcvr on (
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
            From MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
            left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod on pmod.bupiy_id=a.id
            Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
            On pmod.porme_code = mref.porme_code
            And pmod.porce_code = mref.Porce_code
            Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
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
  Left Join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (a.policyid = g.bupiy_id and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached 
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'

-- COMMAND ----------

select * from MI2022.Control1a where policyid=29748

-- COMMAND ----------

select * from MI2022.Control1a where policyid=29748

-- COMMAND ----------

select * from  MI2022.Control1a  where PreCreditPeriodTypePorveCode='SELPCRCO'

-- COMMAND ----------

SELECT COUNT(*) FROM MI2022.Control1a

-- COMMAND ----------

SELECT COUNT(*) FROM MI2022.Control1a

-- COMMAND ----------

SELECT COUNT(*) FROM MI2022.Control1a

-- COMMAND ----------

SELECT COUNT(*) FROM MI2022.Control1a

-- COMMAND ----------

SELECT unit,count(*) FROM MI2022.Control1a group by unit

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where RiskPeriodEndDate > 20191231
  And DataSource = 'SYM'

-- COMMAND ----------

Notes: remove null values

-- COMMAND ----------

select distinct mainproduct from MI2022.Contracts_SL1_20191231_v20220228_01 where unit='GKS Germany'

-- COMMAND ----------

Select
  to_date(cast(a.CoverStartDate as string), 'yyyyMMdd') as CoverStartDate,
  to_date(cast(a.CoverEndDate as string), 'yyyyMMdd') as CoverEndDate,
  g.start_risk_dat as PolicyStartDate, --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate
  g.end_risk_dat as PolicyEndDate -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate
From
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code and mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code and mod.bupiy_id = modVar.Bupiy_id)
  --Drivers
  --MaximumCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
            ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
            AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId
  --MaximumCreditPeriod
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
            ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_cred_per_typ) c on a.PolicyId=c.PolicyId
  --DeviatingCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
            ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id = pol.bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
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
            MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrpcvr on (
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
            From MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
            left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod on pmod.bupiy_id=a.id
            Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
            On pmod.porme_code = mref.porme_code
            And pmod.porce_code = mref.Porce_code
            Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
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
  Left Join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (a.policyid = g.bupiy_id and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached 
  And modvar.porve_code IN ('RAFLG')
  and Current_Date() between mod.effect_from_dat and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat and modvar.effect_to_dat 
  And a.DataSource = 'SYM'
  and a.policyid=36770
  order by a.CoverStartDate asc

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=1003249

-- COMMAND ----------

select CoverStartDate,CoverEndDate from MI2022.Contracts_SL1_20191231_v20220228_01 where   policyid=1000522   order by CoverStartDate asc

-- COMMAND ----------

SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
            ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
            AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null and pol.Bupiy_id = 29748 
            GROUP BY
            pol.bupiy_id,ctrygrp.max_credit_terms_per_typ

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where Bupiy_id = 29748 

-- COMMAND ----------

select * from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 where Bupiy_id = 29748 

-- COMMAND ----------

MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id = a.PolicyId
  Left Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code and mod.porme_Code = refmodvar.porme_code)
  Left Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code and mod.bupiy_id = modVar.Bupiy_id)
  --Drivers
  --MaximumCreditTerms
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
            ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
            AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId

-- COMMAND ----------

select *from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=29748 

-- COMMAND ----------

select * from  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=29748

-- COMMAND ----------

select * from MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 where bupiy_id=29748

-- COMMAND ----------

SELECT
distinct
  a.*,
  pol.bupiy_id as PolicyId,
  ctrygrp.max_credit_terms_per,
  ctrygrp.max_credit_terms_per_typ
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on (
    a.policyid = pol.bupiy_id
    and to_date(pol.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  )
  Inner JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id = pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
where
  policyid=29748
  and contractissuedate=20040101

-- COMMAND ----------

select * from MI2022.Control1a where policyid=29748

-- COMMAND ----------

select pol.Bupiy_id,start_risk_dat, 
Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
from 
MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol 
  Inner JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id = pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
  where pol.bupiy_id=29748
group by 
pol.Bupiy_id,
start_risk_dat,
ctrygrp.max_credit_terms_per_typ

-- COMMAND ----------

with test1 as (
  select
    pol.Bupiy_id,
    start_risk_dat,
    Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
    ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
  from
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
    Inner JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (
      ctrygrp.bupiy_id = pol.Bupiy_id
      AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat
      AND ctrygrp.effect_to_dat
    )
  where
    pol.bupiy_id = 29748
  group by
    pol.Bupiy_id,
    start_risk_dat,
    ctrygrp.max_credit_terms_per_typ
)
select 
PolicyId,ContractissueDate, start_risk_dat,maximumCreditTerms,MaximumCreditTermsType
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join test1 b on (a.policyid = b.Bupiy_id
  and to_date(start_risk_dat, 'yyyyMMdd') = to_date(cast(ContractIssueDate as string), 'yyyyMMdd'))
  where policyid=29748  and RiskPeriodStartDate>20191231

-- COMMAND ----------

select *from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=29748 and RiskPeriodStartDate>20191231
