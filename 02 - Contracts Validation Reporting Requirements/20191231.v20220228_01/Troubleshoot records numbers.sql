-- Databricks notebook source
select count(*) from MI2022.Control3Init

-- COMMAND ----------

-- MAGIC %md the issue isn't from step0

-- COMMAND ----------

select count(*) from (Select  mod.Bupiy_Id, mod.porce_Code, mod.porme_code, refmodvar.porve_code,a.*
From MI2022.Control3Init a left join 
MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on mod.Bupiy_Id=a.PolicyId
Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On mod.porce_code = refmodvar.porce_code
And mod.porme_Code = refmodvar.porme_code
Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar
On refmodvar.porve_Code = modvar.porve_code
And mod.bupiy_id = modVar.Bupiy_id
Where mod.porce_Code = 'C00'
And mod.porme_code IN ('K001', 'K002')
And modvar.porve_code IN ('RAFLG', 'LOFLG'))

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
    ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN  MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id
    ,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId)

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id
    ,ctrygrp.max_cred_per_typ) c on a.PolicyId=c.PolicyId)

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
    ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ) d on a.PolicyId=d.PolicyId)

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(SELECT
    pol.bupiy_id as PolicyId,
    max(ctrygrp.waiting_period) as WaitingPeriod,
    ctrygrp.waiting_period_typ as WaitingPeriodType
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    Where Waiting_period <>0 
    GROUP BY
    pol.Bupiy_id,ctrygrp.waiting_period_typ) e on a.PolicyId=e.PolicyId)

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(Select
  pvar.bupiy_Id as PolicyId,
  refmodvar.porce_code as PreCreditPeriodTypePorceCode,
  refmodvar.porme_code as PreCreditPeriodTypePormecode,
  pvar.porve_Code as PreCreditPeriodTypeProveCode,
  pvar.Value as PreCreditPeriodTypeValue,
  pvar.last_update_dat as PreCreditPeriodTypeLastUpdateDate
  From
  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
  Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On pvar.porve_code = refmodvar.porve_code
  Where
  pvar.porve_code in (
    'MAXPRCR',
    'PERFDAT',
    'PERTYP3',
    'PERTYP',
    'MAXEXPER',
    'POLPER') --and Bupiy_Id = 1000817 
  and pvar.Effect_To_Dat>Current_Date()) f on a.PolicyId=f.PolicyId)

-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(
Select 
    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01  refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  pvar.porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER') 
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)) b on a.PolicyId=b.bupiy_Id)


-- COMMAND ----------

select count(*) from (
select * from
MI2022.Control3Init a
left join(
Select 
    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01  refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  pvar.porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER') and pvar.Effect_To_Dat>Current_Date()
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value
Having pvar.last_update_dat = max(pvar.last_update_dat)) b on a.PolicyId=b.bupiy_Id)


-- COMMAND ----------

Select 
distinct
    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
From MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 pvar
Left Outer Join  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01  refmodvar
On pvar.porve_code = refmodvar.porve_code
Where  pvar.porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER') and pvar.Effect_To_Dat>Current_Date() and pvar.bupiy_Id=210611
Group By    pvar.bupiy_Id, 
    refmodvar.porce_code, 
    refmodvar.porme_code,
    pvar.porve_Code, 
    pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)

-- COMMAND ----------

  select * from MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 where Effect_To_Dat>Current_Date() and bupiy_Id=210611 and porve_code in ('MAXPRCR','PERFDAT','PERTYP3','PERTYP','MAXEXPER','POLPER')

-- COMMAND ----------

select * from  MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 

-- COMMAND ----------

SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms2_per) as DeviatingMaximumCreditTerms,
    ctrygrp.max_credit_terms2_per_typ as DeviatingMaximumCreditTermsType
    FROM MI2022.Contracts_SL1_20191231_v20220228_01 a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    where pol.bupiy_id=1000817 and ctrygrp.max_credit_terms2_per_typ is not null
    GROUP BY
    pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ
