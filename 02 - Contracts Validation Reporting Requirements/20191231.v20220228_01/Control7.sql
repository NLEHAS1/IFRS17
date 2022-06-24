-- Databricks notebook source
-- MAGIC %md The code below define the non-modula policies in Tbbu_Policies

-- COMMAND ----------

select * from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where Bupty_Typ <> 'CIM'

-- COMMAND ----------

-- MAGIC %md Uli 17-March-2022 new code to extract non-modula policies

-- COMMAND ----------

Select
  Bupiy_id,
  Count(PORME_CODE)
From
  MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01
where
Group By
  Bupiy_Id
Having Count(PORME_CODE)=1

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Create view of Contracts table for non modula policies

-- COMMAND ----------

drop view MI2022.Control7

-- COMMAND ----------

create view MI2022.Control7 as
select * from
  (Select
      Bupiy_id,
      Count(PORME_CODE)
    From
      MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01
    Group By
      Bupiy_Id
    Having
      Count(PORME_CODE) = 1 ) a  left join MI2022.Contracts_SL1_20191231_v20220228_01 b on a.Bupiy_id = b.PolicyId
where
   b.datasource = 'SYM'
  and b.RiskPeriodEndDate <= 20191231

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid in (Select
      Bupiy_id
    From
      MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01
    Group By
      Bupiy_Id
    Having
      Count(PORME_CODE) = 1 ) and RiskPeriodEndDate > 20191231

-- COMMAND ----------

select count(*) from MI2022.Control7 

-- COMMAND ----------

select * from MI2022.Control7 

-- COMMAND ----------

select count(*) from MI2022.Control7 

-- COMMAND ----------

select * from MI2022.Control7 

-- COMMAND ----------

select id,policyid from MI2022.Control7 where id=467685

-- COMMAND ----------

-- MAGIC %md According to Control 7 req: List for (new) Non-Modula policies (first record) whether it is RA or LA. However looking at Policies features for Non Modula policies we find it's neither Risk attached or loss ocurreing

-- COMMAND ----------

select distinct mod.porce_Code,mod.porme_code,modvar.porve_code
from
  MI2022.NonModulaPolicies a
   join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on a.PolicyId = mod.Bupiy_Id 
     Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code And mod.porme_Code = refmodvar.porme_code)
   Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code And mod.bupiy_id = modVar.Bupiy_id)
  -- 

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=1002514

-- COMMAND ----------

Select
  *
From
  MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01
where Bupiy_id=1002514


