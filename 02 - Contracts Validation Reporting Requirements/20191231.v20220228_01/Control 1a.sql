-- Databricks notebook source
SELECT
  a.ID as POLICY_ID,
  a.CMPCT_ID as CMPCT_ID,
  a.d_popss_status_code as POLICY_STATUS,
  b.PORME_CODE as BREAK_MODULE_CODE,
  b.EFFECT_FROM_DAT as EFFECTIVE_FROM,
  b.EFFECT_TO_DAT as EFFECTIVE_TO
FROM
  sourcedata_ci_symphony_tbbu_policies a,
  sourcedata_ci_symphony_tbpo_pol_modules b
WHERE
  a.ID = b.BUPIY_ID
  AND a.d_popss_status_code in ('LIVE', 'SUSP')
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
order by
  a.ID

-- COMMAND ----------

-- MAGIC %md General Items

-- COMMAND ----------

select * from contracts where policyid=30001491 and ContractIssueDate=20011101 

-- COMMAND ----------

select count(*) from contracts where RiskPeriodEndDate >20191231 and datasource='SYM'

-- COMMAND ----------

-- MAGIC %md Step 0: Select risk attaching policies

-- COMMAND ----------

Select mod.Bupiy_Id, mod.porce_Code, mod.porme_code, refmodvar.porve_code, modvar.*,a.*
From 
MI2022.Contracts_SL1_20191231_v20220228_01 a left join 
MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on a.PolicyId=mod.Bupiy_Id
Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar
On mod.porce_code = refmodvar.porce_code
And mod.porme_Code = refmodvar.porme_code
Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar
On refmodvar.porve_Code = modvar.porve_code
And mod.bupiy_id = modVar.Bupiy_id
Where mod.Bupiy_Id = 1000817
and mod.porce_Code = 'C00'
And mod.porme_code IN ('K001', 'K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
And modvar.porve_code IN ('RAFLG', 'LOFLG')


-- COMMAND ----------

select count(*) from (Select mod.Bupiy_Id, mod.porce_Code, mod.porme_code, refmodvar.porve_code, modvar.*,a.*
From 
db_201912_20220222_091937_113.Contracts a left join 
db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_modules mod on a.PolicyId=mod.Bupiy_Id
Left Outer Join db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_ref_module_variable_types refmodvar
On mod.porce_code = refmodvar.porce_code
And mod.porme_Code = refmodvar.porme_code
Left Outer Join db_sl1_20211001_run13.sourcedata_ci_symphony_TBPO_POL_MOD_VARIABLES modvar
On refmodvar.porve_Code = modvar.porve_code
And mod.bupiy_id = modVar.Bupiy_id
--Where mod.Bupiy_Id = 1126118
Where mod.porce_Code = 'C00'
And mod.porme_code IN ('K001', 'K002') --K001 (`RAFLG´) represents Risk Attached and K002 (`LOFLG´) Loss Occurring
And modvar.porve_code IN ('RAFLG', 'LOFLG')
)

-- COMMAND ----------

-- MAGIC %md Step 1: Off each policy select the first contract and calculate the length of coverageperiod (in number of months/days) and order the policies from short period to long period

-- COMMAND ----------

select policyid,contractid,ContractIssueDate, 
to_date(cast(CoverEndDate as string),'yyyyMMdd') as CoverEndDate,
to_date(cast(CoverStartDate as string),'yyyyMMdd') as CoverStartDate,
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as DaysDiff,
months_between(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as MontDiff, 
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') )/360 as YearsDiff,a.*
from db_201912_20220222_091937_113.Contracts a where CoverStartDate=ContractIssueDate and DataSource='SYM'
order by 6 desc

-- COMMAND ----------

select Mainunit, count(*) from (select policyid,contractid,ContractIssueDate, 
to_date(cast(CoverEndDate as string),'yyyyMMdd') as CoverEndDate,
to_date(cast(CoverStartDate as string),'yyyyMMdd') as CoverStartDate,
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as DaysDiff,
months_between(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') ) as MontDiff, 
DATEDIFF(to_date(cast(CoverEndDate as string),'yyyyMMdd'),to_date(cast(CoverStartDate as string),'yyyyMMdd') )/360 as YearsDiff,a.*
from db_201912_20220222_091937_113.Contracts a where CoverStartDate=ContractIssueDate and DataSource='SYM') group by mainunit

-- COMMAND ----------

-- MAGIC %md Step 2: List the 10% policies (or 5% policies depending of size of books) with longest coverage period and ask unit to check those ----> Check if that can be done in PBI

-- COMMAND ----------

-- MAGIC %md #####Step 3: To the list add contract features which are drivers behind the coverenddate: like(Maximal Credit Term, MEP, Waiting Period, Binding Cover, PCR, Runoff cover) as all these have impact on CoverEndDate

-- COMMAND ----------

-- MAGIC %md %#MaximumCreditTerms 

-- COMMAND ----------

SELECT
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_credit_terms_per) as maximumCreditTerms,
ctrygrp.max_credit_terms_per_typ as MaximumCreditTermsType
FROM db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_versions pol
JOIN  db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_ctry_grp_mcts ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
--AND #ContractStartDate BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
GROUP BY
pol.bupiy_id
,ctrygrp.max_credit_terms_per_typ

-- COMMAND ----------

-- MAGIC %md %#MaximumCreditPeriod 

-- COMMAND ----------

SELECT
pol.bupiy_id as PolicyId,
Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
ctrygrp.max_cred_per_typ as MaximumCreditTermsType
FROM db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_versions pol
JOIN db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_ctry_grp_mcts ctrygrp on ctrygrp.bupiy_id=pol.Bupiy_id 
--AND #ContractStartDate BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
GROUP BY
pol.bupiy_id
,ctrygrp.max_cred_per_typ


-- COMMAND ----------

-- MAGIC %md %#DeviatingCreditTerms

-- COMMAND ----------

SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_credit_terms2_per) as maximumCreditTerms,
    ctrygrp.max_credit_terms2_per_typ as MaximumCreditTermsType
FROM db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_versions pol
JOIN db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_ctry_grp_mcts ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
-- AND #contractStartDate BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat
GROUP BY
pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ 


-- COMMAND ----------

-- MAGIC %md %#WaitingPeriod

-- COMMAND ----------

SELECT
    pol.bupiy_id as PolicyId,
    max(ctrygrpcvr.waiting_period) as WaitingPeriod,
    ctrygrpcvr.waiting_period_typ as WaitingPeriodType
FROM db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_versions pol
JOIN db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_ctry_grp_cover_pcts ctrygrpcvr on ctrygrpcvr.bupiy_id=pol.bupiy_id
--AND #ContractStartDate BETWEEN ctrygrpcvr.effect_from_dat AND ctrygrpcvr.effect_to_dat
Where Waiting_period <>0 
 GROUP BY
pol.Bupiy_id,ctrygrpcvr.waiting_period_typ


-- COMMAND ----------

-- MAGIC %md %#ModuleNumber ---> What is the source of MappingTable

-- COMMAND ----------

select * from contracts where datasource='SYM'

-- COMMAND ----------

SELECT FromObjectValue from MappingTable WHERE
AND ToObjectType='ModuleLabel'  
AND FromType='ModuleNumber'
AND ToObjectValue='PreCreditRisk'
AND #ValuationDate BETWEEN effect_from_dat AND effect_to_dat


-- COMMAND ----------

-- MAGIC %md %#ModuleVariableName ---> What is the source of MappingTable

-- COMMAND ----------

SELECT FromObjectValue from MappingTable WHERE
AND ToObjectType='ModuleLabel'
AND FromType='ModuleVariableName'
AND ToObjectValue='PreCreditRisk'
AND valuationdate BETWEEN effect_from_dat AND effect_to_dat


-- COMMAND ----------

select c.obj_value as FromObjectValue 
from
MI2022.TBIF_REF_OBJ_TYP_SL1_20191231_v20220228_01 a,
MI2022.TBIF_REF_OBJ_MAPPING_SL1_20191231_v20220228_01 b,
MI2022.TBIF_REF_OBJ_SL1_20191231_v20220228_01 c
where a.id=b.id and a.id=c.id and b.id=c.id
and a.name='ModuleLabel'


-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_SL1_20191231_v20220228_01 c where c.obj_value='MAXPRCR'

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_SL1_20191231_v20220228_01 c where c.obj_value='PreCreditRisk'

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_TYP_SL1_20191231_v20220228_01 where name='ModuleLabel'

-- COMMAND ----------

select * from MI2022.TBIF_REF_OBJ_TYP_SL1_20191231_v20220228_01 where name='ModuleVariableName'

-- COMMAND ----------

-- MAGIC %md %#NoCoverPeriod --> What is the source of PolicyStartDate and PolicyEndDate

-- COMMAND ----------

IF #PolicyStartDate=#PolicyEndDate THEN TRUE ELSE FALSE END

-- COMMAND ----------

-- MAGIC %md %#PreCreditPeriod --> I don't have access to MappingTable, what is #FirstDayOfContract

-- COMMAND ----------

SELECT 
value 
FROM 
db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_mod_variables polmod
WHERE 
porve_code=#ModuleVariableName
AND
#FirstDayOfContract BETWEEN effect_from_dat AND effect_to_dat
AND polmod.bupiy_id=#PolicyNumber


-- COMMAND ----------

select * from db_sl1_20211001_run13.sourcedata_ci_symphony_tbpo_pol_mod_variables where porve_code=''

-- COMMAND ----------

-- MAGIC %md %#PreCreditPeriodType 

-- COMMAND ----------

Select
  pvar.bupiy_Id,
  refmodvar.porce_code,
  refmodvar.porme_code,
  pvar.porve_Code,
  pvar.Value,
  pvar.last_update_dat
From
  db_sl1_20211001_run13.sourcedata_ci_symphony_TBPO_POL_MOD_VARIABLES pvar
  Left Outer Join db_sl1_20211001_run13.sourcedata_ci_symphony_TBPO_REF_MODULE_VARIABLE_TYPES refmodvar On pvar.porve_code = refmodvar.porve_code
Where
  pvar.porve_code in (
    'MAXPRCR',
    'PERFDAT',
    'PERTYP3',
    'PERTYP',
    'MAXEXPER',
    'POLPER') --and Bupiy_Id = 1014
Group By
  pvar.bupiy_Id,
  refmodvar.porce_code,
  refmodvar.porme_code,
  pvar.porve_Code,
  pvar.Value,
    pvar.last_update_dat
Having pvar.last_update_dat = max(pvar.last_update_dat)    


-- COMMAND ----------

-- MAGIC %md Fix to the Code above

-- COMMAND ----------

Select
  pvar.bupiy_Id,
  refmodvar.porce_code,
  refmodvar.porme_code,
  pvar.porve_Code,
  pvar.Value,
  pvar.last_update_dat
From
  db_sl1_20211001_run13.sourcedata_ci_symphony_TBPO_POL_MOD_VARIABLES pvar
  Left Outer Join db_sl1_20211001_run13.sourcedata_ci_symphony_TBPO_REF_MODULE_VARIABLE_TYPES refmodvar On pvar.porve_code = refmodvar.porve_code
Where
  pvar.porve_code in (
    'MAXPRCR',
    'PERFDAT',
    'PERTYP3',
    'PERTYP',
    'MAXEXPER',
    'POLPER') --and Bupiy_Id = 1000817 
and pvar.Effect_To_Dat>Current_Date()


-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

-- MAGIC %md Questions:
-- MAGIC 1. Type of join between Contracts and the rest of SYM tables
-- MAGIC 2.The type of join between tbpo_pol_versions & tbpo_ctry_grp_mcts ctrygrp and the rest of table

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 a where a.RiskPeriodEndDate >20191231 and a.DataSource='SYM'

-- COMMAND ----------

DESC MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01

-- COMMAND ----------

drop view MI2022.Control1aInt

-- COMMAND ----------

--Create view MI2022.Control1aInt as
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
  --f.PreCreditPeriodTypePorceCode,
  --f.PreCreditPeriodTypePormecode,
  --f.PreCreditPeriodTypeProveCode,
  --f.PreCreditPeriodTypeValue,
  --f.PreCreditPeriodTypeLastUpdateDate,
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
  /* Left join (Select                  --Removed because of the duplicates and the code isn't defined correctly
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
    'POLPER') --and Bupiy_Id = 1000817 
  and pvar.Effect_To_Dat>Current_Date()) f on a.PolicyId=f.PolicyId */
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
Where
  mod.porce_Code = 'C00'
  And mod.porme_code IN ('K001') --K001 (`RAFLG´) represents Risk Attached
  And modvar.porve_code IN ('RAFLG')
  And a.RiskPeriodEndDate > 20191231
  And a.DataSource = 'SYM'
Order by CoveragePeriodDaysDiff desc

-- COMMAND ----------

select count(*) from MI2022.Control1aInt

-- COMMAND ----------

select count(*) from MI2022.Control1aInt

-- COMMAND ----------

select count(*) from MI2022.Control1aInt

-- COMMAND ----------

select count(*) from MI2022.Control1aIntsc

-- COMMAND ----------

drop view  MI2022.Control1a

-- COMMAND ----------

create view MI2022.Control1a as 
select * from MI2022.Control1aInt TABLESAMPLE (10 PERCENT)

-- COMMAND ----------

select count(*) from MI2022.Control1a

-- COMMAND ----------

select count(*) from MI2022.Control1a

-- COMMAND ----------

select count(*) from MI2022.Control1a

-- COMMAND ----------

select count(*) from MI2022.Control1a

-- COMMAND ----------

select count(*) from MI2022.Control1a

-- COMMAND ----------

select * from MI2022.Control1a where NoCoverPeriod=true

-- COMMAND ----------

select unit,count(*) from MI2022.Control1aInt group by unit

-- COMMAND ----------

select unit,count(*) from MI2022.Control1a group by unit

-- COMMAND ----------

select unit,count(*) from (select * from MI2022.Control1aInt TABLESAMPLE (10 PERCENT)) group by unit

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=1000001

-- COMMAND ----------

select id,d_popvn_start_risk_dat,d_popvn_end_risk_dat from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=1000001

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01
