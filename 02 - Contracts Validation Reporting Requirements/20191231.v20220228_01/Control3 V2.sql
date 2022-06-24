-- Databricks notebook source
drop view MI2022

-- COMMAND ----------

drop view MI2022.Control3

-- COMMAND ----------

Create view MI2022.Control3 as
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
      mvar.EFFECT_FROM_DAT as EFFECT_FROM_DAT_1,
      mvar.Effect_to_dat as Effect_To_Dat_1,
      mvar.value
    From
      MI2022.Contracts_SL1_20191231_v20220228_01 a
      left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod on a.PolicyId = pmod.Bupiy_Id
      Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref On (
        pmod.porme_code = mref.porme_code
        And pmod.porce_code = mref.Porce_code
      )
      Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar On (
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
      And a.RiskPeriodEndDate > 20191231
      And a.DataSource = 'SYM'
      and cancellability != 0
  ) pivot (
    max(value) for porve_code in (
      'NROFDAYS' as Withrawal_Days,
      'NROFMONT' as Withdawal_Month
    )
  )

-- COMMAND ----------

select distinct porve_code from MI2022.Control3

-- COMMAND ----------

select count(*) from MI2022.Control3

-- COMMAND ----------

select count(*) from MI2022.Control3

-- COMMAND ----------

select count(*) from MI2022.Control3

-- COMMAND ----------

select count(*) from MI2022.Control3

-- COMMAND ----------

-- MAGIC %md Uli-- BindingContractCover and NonCancellation 15.03.2022

-- COMMAND ----------

Select
pmod.Bupiy_Id,
pmod.Effect_from_dat,
pmod.Effect_to_Dat,
pmod.Porce_code,
pmod.porme_code,
mvar.PORVE_CODE,
mvar.EFFECT_FROM_DAT,
mvar.Effect_to_dat,
mvar.value
From MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod
Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref On (pmod.porme_code = mref.porme_code And pmod.porce_code = mref.Porce_code)
Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar On (pmod.Bupiy_Id = mvar.Bupiy_Id And mref.Porve_code = mvar.porve_Code)
Where Current_date() between pmod.effect_from_dat and pmod.effect_to_dat
And Current_date() between mvar.effect_from_dat and mvar.effect_to_dat
And
( pmod.PORME_CODE like ('15200.%')
OR pmod.PORME_CODE like ('15206.%')
OR pmod.PORME_CODE like ('00105.%') -- MBNCIND = MBNC Policy Indicator
)

-- COMMAND ----------

select * from MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 where Bupiy_Id=240910

-- COMMAND ----------

select * from
MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 pmod 
      Left Outer Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 mref On (
        pmod.porme_code = mref.porme_code
        And pmod.porce_code = mref.Porce_code
      )
      Left Outer Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 mvar On (
        pmod.Bupiy_Id = mvar.Bupiy_Id
        And mref.Porve_code = mvar.porve_Code
      ) where pmod.Bupiy_Id=240910
