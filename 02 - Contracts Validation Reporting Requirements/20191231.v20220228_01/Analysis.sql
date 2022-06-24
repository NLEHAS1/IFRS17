-- Databricks notebook source
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
Where pmod.Bupiy_id = 997595
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
))
pivot (max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in ('PERTYP3','MAXPRCR'))

-- COMMAND ----------

select distinct PreCreditPeriodTypePorveCode from (Select
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
Where 
 a.d_popvn_end_risk_dat between pmod.effect_from_dat and pmod.effect_to_dat
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
Where  a.d_popvn_end_risk_dat between pmod.effect_from_dat and pmod.effect_to_dat
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
pivot (max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in ('PERTYP3','MAXPRCR','PERFDAT','SELPCRCO'))

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

select distinct PreCreditPeriodTypePorveCode from MI2022.Control1b

-- COMMAND ----------

select *  from MI2022.Control1b

-- COMMAND ----------

desc MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01

-- COMMAND ----------

desc MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM' and RiskPeriodEndDate > 20191231
