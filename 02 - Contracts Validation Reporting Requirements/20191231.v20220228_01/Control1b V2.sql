-- Databricks notebook source
-- MAGIC %md #Step 0: Select loss occurring policies

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
  And mod.porme_code IN ('K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('LOFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'

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
  And mod.porme_code IN ('K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('LOFLG')
  and Current_Date() between mod.effect_from_dat
  and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat
  and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'
  )

-- COMMAND ----------

-- MAGIC %md #Step 1: Off each policy select the last contract of the cohort and define a flag which is TRUE if cover end date equals policy end date

-- COMMAND ----------

select a.*,
case
when a.policyid = b.policyid and a.CoverStartDate=b.CoverStartDate and  a.CoverEndDate = a.RiskPeriodEndDate then True
Else False
End as Step1
from MI2022.Contracts_SL1_20191231_v20220228_01 a
left join (select policyid,max(CoverStartDate) as CoverStartDate from  MI2022.Contracts_SL1_20191231_v20220228_01 where  CoverEndDate = RiskPeriodEndDate and datasource='SYM' group by policyid) b on a.policyid=b.policyid
where datasource='SYM' and RiskPeriodEndDate > 20191231


-- COMMAND ----------

-- MAGIC %md #Step 3 To the list add contract features which are drivers behind the coverenddate: like(Maximal Credit Term, MEP, Waiting Period, Binding Cover, PCR, Runoff cover) as all these have impact on CoverEndDate.

-- COMMAND ----------

-- MAGIC %md ###Runoffcover

-- COMMAND ----------

Select 
    pvar.bupiy_Id as PolicyId, 
    refmodvar.porce_code as RunoffCoverPorceCode, 
    refmodvar.porme_code as RunoffCoverPormeCode,
    pvar.porve_Code as RunoffCoverPorveCode, 
    pvar.Value as RunoffCoverValue,
    pvar.last_update_dat as RunoffCoverLastUpdateDat
From  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01  pol 
left join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar on pol.bupiy_Id=pvar.bupiy_Id and pol.end_risk_dat BETWEEN pvar.effect_from_dat AND pvar.effect_to_dat
Left  Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On pvar.porve_code = refmodvar.porve_code
Where pvar.bupiy_Id=215547 and (refmodvar.porme_code LIKE ('%04200%') OR  refmodvar.porme_code LIKE ('%04600%') OR  refmodvar.porme_code LIKE ('%04700%') OR  refmodvar.porme_code LIKE ('%04800%') OR  refmodvar.porme_code LIKE ('%05000%')  OR  refmodvar.porme_code LIKE ('%05001%') OR  refmodvar.porme_code LIKE ('%05005%') OR  refmodvar.porme_code LIKE ('%0510%') OR  refmodvar.porme_code LIKE ('%05100%'))
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)    


-- COMMAND ----------

select policyid,count(*) from (
Select 
    pvar.bupiy_Id as PolicyId, 
    refmodvar.porce_code as RunoffCoverPorceCode, 
    refmodvar.porme_code as RunoffCoverPormeCode,
    pvar.porve_Code as RunoffCoverPorveCode, 
    pvar.Value as RunoffCoverValue,
    pvar.last_update_dat as RunoffCoverLastUpdateDat
From 
MI2022.Contracts_SL1_20191231_v20220228_01 a left join 
MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar on a.PolicyId=pvar.bupiy_Id
Left  Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  refmodvar.porme_code LIKE ('%04200%') OR  refmodvar.porme_code LIKE ('%04600%') OR  refmodvar.porme_code LIKE ('%04700%') OR  refmodvar.porme_code LIKE ('%04800%') OR  refmodvar.porme_code LIKE ('%05000%')  OR  refmodvar.porme_code LIKE ('%05001%') OR  refmodvar.porme_code LIKE ('%05005%') OR  refmodvar.porme_code LIKE ('%0510%') OR  refmodvar.porme_code LIKE ('%05100%')
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)    ) group by policyid having count(*)>1 order by 2 desc


-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

drop view MI2022.Control1b

-- COMMAND ----------

Create view MI2022.Control1b as 
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
  to_date(cast(a.ContractInceptionDate as string), 'yyyyMMdd') as ContractInceptionDate,
  to_date(cast(a.RiskPeriodStartDate as string), 'yyyyMMdd') as RiskPeriodStartDate,
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
  d.DeviatingMaximumCreditTerms,
  d.DeviatingMaximumCreditTermsType,
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
  when a.policyid = g.policyid and a.CoverStartDate=g.CoverStartDate and  a.CoverEndDate = a.RiskPeriodEndDate then True
  Else False
  End as LastContractCohort,
  h.RunoffCoverPorceCode, 
  h.RunoffCoverPormeCode,
  h.RunoffCoverPorveCode, 
  h.RunoffCoverValue,
  h.RunoffCoverLastUpdateDat,
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
            ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType,pol.start_risk_dat
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id 
            AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_credit_terms_per is not null and ctrygrp.max_credit_terms_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_credit_terms_per_typ,pol.start_risk_dat) b on a.PolicyId=b.PolicyId and to_date(b.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string))
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
  --LastContractCohort
  left join (select policyid,max(CoverStartDate) as CoverStartDate from  MI2022.Contracts_SL1_20191231_v20220228_01 where  CoverEndDate = RiskPeriodEndDate and datasource='SYM' group by policyid)g on a.policyid=g.policyid
  --RunoffCover 
  left join (Select 
            pvar.bupiy_Id as PolicyId, 
            refmodvar.porce_code as RunoffCoverPorceCode, 
            refmodvar.porme_code as RunoffCoverPormeCode,
            pvar.porve_Code as RunoffCoverPorveCode, 
            pvar.Value as RunoffCoverValue,
            pvar.last_update_dat as RunoffCoverLastUpdateDat
            From  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01  pol 
            left join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar on 
            (pol.bupiy_Id=pvar.bupiy_Id and pol.end_risk_dat BETWEEN pvar.effect_from_dat AND pvar.effect_to_dat)
            Left  Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
            On pvar.porve_code = refmodvar.porve_code
            Where pvar.bupiy_Id=215547 and (refmodvar.porme_code LIKE ('%04200%') OR  
            refmodvar.porme_code LIKE ('%04600%') OR  refmodvar.porme_code LIKE ('%04700%') OR  
            refmodvar.porme_code LIKE ('%04800%') OR  refmodvar.porme_code LIKE ('%05000%')  OR  
            refmodvar.porme_code LIKE           ('%05001%') OR  refmodvar.porme_code LIKE ('%05005%') OR  
            refmodvar.porme_code LIKE ('%0510%') OR  refmodvar.porme_code LIKE ('%05100%'))
            Group By    pvar.bupiy_Id, 
            refmodvar.porce_code, 
            refmodvar.porme_code,
            pvar.porve_Code, 
            pvar.Value,
            pvar.last_update_dat
            Having pvar.last_update_dat = max(pvar.last_update_dat) ) h on a.policyid=h.policyid
  --NoCoverPeriod
  Left Join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (a.policyid = g.bupiy_id and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
Where
   mod.porce_Code = 'C00'
  And mod.porme_code IN ('K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('LOFLG')
  and Current_Date() between mod.effect_from_dat and mod.effect_to_dat
  and Current_Date() between modvar.effect_from_dat and modvar.effect_to_dat 
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

-- MAGIC %md I finish Control 1a and 1 b check the code again and build the reports based on the guide
