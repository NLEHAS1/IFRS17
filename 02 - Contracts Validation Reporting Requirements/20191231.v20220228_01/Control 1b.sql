-- Databricks notebook source
-- MAGIC %md Step 0 is the same between Contral 1a and 1b so I will use the view I created

-- COMMAND ----------

-- MAGIC %md Step 1: Off each policy select the last contract of the cohort and define a flag which is TRUE if cover end date equals policy end date

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

-- MAGIC %md Step 2: Step 2: List all policies where the flag is FALSE and ask unit to check those ---> Can be done in PBI

-- COMMAND ----------

-- MAGIC %md Step 3 To the list add contract features which are drivers behind the coverenddate: like(Maximal Credit Term, MEP, Waiting Period, Binding Cover, PCR, Runoff cover) as all these have impact on CoverEndDate.
-- MAGIC For the list of items to added see 1a) plus RunoffCover is relevant (mnot part of 1a). To select Runoffcover. 

-- COMMAND ----------

Select 
    pvar.bupiy_Id as PolicyId, 
    refmodvar.porce_code as RunoffCoverPorceCode, 
    refmodvar.porme_code as RunoffCoverPormeCode,
    pvar.porve_Code as RunoffCoverPorveCode, 
    pvar.Value as RunoffCoverValue,
    pvar.last_update_dat as RunoffCoverLastUpdateDat
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  refmodvar.porme_code LIKE ('%04200%') OR  refmodvar.porme_code LIKE ('%04600%') OR  refmodvar.porme_code LIKE ('%04700%') OR  refmodvar.porme_code LIKE ('%04800%') OR  refmodvar.porme_code LIKE ('%05000%')  OR  refmodvar.porme_code LIKE ('%05001%') OR  refmodvar.porme_code LIKE ('%05005%') OR  refmodvar.porme_code LIKE ('%0510%') OR  refmodvar.porme_code LIKE ('%05100%')
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)    


-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

-- MAGIC %md Integrate Contral1a and Control1b

-- COMMAND ----------

drop view MI2022.Control1b

-- COMMAND ----------

-- MAGIC %md Last update I add the last SQL code shared by Uli to use for PreCreditPeriodType. That decrease the duplicates. Check with him the output before applying the changes in Control1a

-- COMMAND ----------

Create view MI2022.Control1b as
select distinct
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
  mod.porce_Code,
  mod.porme_code,
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
  f.PreCreditPeriodTypePorceCode,
  f.PreCreditPeriodTypePormecode,
  f.PreCreditPeriodTypePorveCode,
  f.PreCreditPeriodTypeValue,
  f.PreCreditPeriodTypeEffectFromDate,
  f.PreCreditPeriodTypeEffectToDate,
  modvar.effect_from_dat,
  modvar.effect_to_dat,
  modvar.value,
  modvar.orsus_id,
  modvar.last_update_dat,
  modvar.orsus_create_id,
  modvar.create_dat,
  modvar.change_dat,
  modvar.seq,
  modvar.origin_ind,
  modvar.unicode_value,
  case
  when a.CoverEndDate = a.RiskPeriodEndDate and a.CoverStartDate=g.CoverStartDate then True
  Else False
  End as LastContractCohort,
  h.RunoffCoverPorceCode, 
  h.RunoffCoverPormeCode,
  h.RunoffCoverPorveCode, 
  h.RunoffCoverValue,
  h.RunoffCoverLastUpdateDat,
  i.d_popvn_start_risk_dat as PolicyStartDate, --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate
  i.d_popvn_end_risk_dat as PolicyEndDate -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on a.PolicyId = mod.Bupiy_Id
  Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code And mod.porme_Code = refmodvar.porme_code)
  Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code And mod.bupiy_id = modVar.Bupiy_id)
  left join(SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
    ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
    FROM MI2022.Contracts_SL1_20191231_v20220228_01 a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN  MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id
    ,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId
  left join (SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
    FROM MI2022.Contracts_SL1_20191231_v20220228_01 a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id
    ,ctrygrp.max_cred_per_typ) c on a.PolicyId=c.PolicyId
  left join (SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
    ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
    FROM MI2022.Contracts_SL1_20191231_v20220228_01 a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    where ctrygrp.max_credit_terms2_per_typ is not null
    GROUP BY
    pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ) d on a.PolicyId=d.PolicyId
  Left join (SELECT
    pol.bupiy_id as PolicyId,
    max(ctrygrp.waiting_period) as WaitingPeriod,
    ctrygrp.waiting_period_typ as WaitingPeriodType
    FROM MI2022.Contracts_SL1_20191231_v20220228_01 a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    Where Waiting_period <>0 
    GROUP BY
    pol.Bupiy_id,ctrygrp.waiting_period_typ) e on a.PolicyId=e.PolicyId
    Left join (Select
    pmod.Bupiy_Id as PolicyId,
    pmod.Effect_from_dat as PreCreditPeriodTypeEffectFromDate,
    pmod.Effect_to_Dat as PreCreditPeriodTypeEffectToDate,
    pmod.Porce_code as PreCreditPeriodTypePorceCode,
    pmod.porme_code as PreCreditPeriodTypePormeCode,
    mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
    mvar.EFFECT_FROM_DAT,
    mvar.Effect_to_dat,
    mvar.value as PreCreditPeriodTypeValue
    From MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod
    Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
    On pmod.porme_code = mref.porme_code
    And pmod.porce_code = mref.Porce_code
    Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
    On pmod.Bupiy_Id = mvar.Bupiy_Id 
    And mref.Porve_code = mvar.porve_Code
    where
    pmod.effect_to_dat > current_date()
    And mvar.effect_to_dat > current_date()
    And mvar.Porve_Code In ('MAXPRCR','PERFDAT','PERTYP3')) f on a.PolicyId=f.PolicyId 
  left join (select policyid,max(CoverStartDate) as CoverStartDate from  MI2022.Contracts_SL1_20191231_v20220228_01 where  CoverEndDate = RiskPeriodEndDate and datasource='SYM' group by policyid)g on a.policyid=g.policyid
  left join (
  Select 
    pvar.bupiy_Id as PolicyId, 
    refmodvar.porce_code as RunoffCoverPorceCode, 
    refmodvar.porme_code as RunoffCoverPormeCode,
    pvar.porve_Code as RunoffCoverPorveCode, 
    pvar.Value as RunoffCoverValue,
    pvar.last_update_dat as RunoffCoverLastUpdateDat
  From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
  Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
  On pvar.porve_code = refmodvar.porve_code
  Where  refmodvar.porme_code LIKE ('%04200%') OR  refmodvar.porme_code LIKE ('%04600%') OR  refmodvar.porme_code LIKE ('%04700%') OR  refmodvar.porme_code LIKE ('%04800%') OR  refmodvar.porme_code LIKE ('%05000%')  OR  refmodvar.porme_code LIKE ('%05001%')   OR  refmodvar.porme_code LIKE ('%05005%') OR  refmodvar.porme_code LIKE ('%0510%') OR  refmodvar.porme_code LIKE ('%05100%')
  Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
  Having pvar.last_update_dat = max(pvar.last_update_dat)) h on a.policyid=h.policyid
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K002')-- K002 (`LOFLG´) Loss Occurring
  And modvar.porve_code IN ('LOFLG')
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'
  And a.CoverEndDate = a.RiskPeriodEndDate ---> This condition to meet Control1b step 1 req
  and modvar.effect_to_dat>Current_Date()
Order by CoveragePeriodDaysDiff desc

-- COMMAND ----------



-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select count(*) from MI2022.Control1b

-- COMMAND ----------

select * from MI2022.Control1b where policyid=1000817

-- COMMAND ----------

select * from MI2022.Control1b where policyid=1000817

-- COMMAND ----------

Select
    pmod.Bupiy_Id as PolicyId,
    pmod.Effect_from_dat as PreCreditPeriodTypeEffectFromDate,
    pmod.Effect_to_Dat as PreCreditPeriodTypeEffectToDate,
    pmod.Porce_code as PreCreditPeriodTypePorceCode,
    pmod.porme_code as PreCreditPeriodTypePormeCode,
    mvar.PORVE_CODE as PreCreditPeriodTypePorveCode,
    mvar.EFFECT_FROM_DAT,
    mvar.Effect_to_dat,
    mvar.value as PreCreditPeriodTypeValue
    From MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod
    Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref
    On pmod.porme_code = mref.porme_code
    And pmod.porce_code = mref.Porce_code
    Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar
    On pmod.Bupiy_Id = mvar.Bupiy_Id 
    And mref.Porve_code = mvar.porve_Code
    where
    And  pmod.effect_to_dat > current_date()
    And mvar.effect_to_dat > current_date()
    And mvar.Porve_Code In ('MAXPRCR','PERFDAT','PERTYP3')    

-- COMMAND ----------

Select            
    pvar.bupiy_Id as PolicyId,
    refmodvar.porce_code as PreCreditPeriodTypePorceCode,
    refmodvar.porme_code as PreCreditPeriodTypePormecode,
    pvar.porve_Code as PreCreditPeriodTypeProveCode,
    pvar.Value as PreCreditPeriodTypeValue,
    pvar.last_update_dat as PreCreditPeriodTypeLastUpdateDate
  From
  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
  Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On pvar.porve_code = refmodvar.porve_code
  Where
  pvar.porve_code in (
    'MAXPRCR',
    'PERFDAT',
    'PERTYP3',
    'PERTYP',
    'MAXEXPER',
    'POLPER') 
  and pvar.Effect_To_Dat>Current_Date() and pvar.bupiy_Id=1000817

-- COMMAND ----------

select * from MI2022.Control1b where policyid=1000817

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=1000817

-- COMMAND ----------

select distinct policyid from MI2022.Contracts_SL1_20191231_v20220228_01 

-- COMMAND ----------

select * from MI2022.Control1b where policyid=1000817

-- COMMAND ----------

Select 
    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  pvar.porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER') and Bupiy_Id = 1000817
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)    


-- COMMAND ----------

Select 
    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  pvar.porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER') and Bupiy_Id = 1000817 and Effect_To_Dat>Current_Date()



-- COMMAND ----------

select bupiy_id,porve_code,effect_from_dat,effect_to_dat,value,last_update_dat from MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 where Bupiy_Id = 1000817 and porve_code in ('MAXPRCR','PERTYP3','PERFDAT') and Effect_To_Dat>Current_Date()

-- COMMAND ----------

select
  bupiy_id,
  porve_code,
  value,
  max(last_update_dat)
from
  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01
where
  Bupiy_Id = 1000817
  and porve_code in ('MAXPRCR', 'PERTYP3')
group by
  bupiy_id,
  porve_code,
  value
