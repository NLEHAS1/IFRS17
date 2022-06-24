-- Databricks notebook source
select * from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0

-- COMMAND ----------

select
  policyid,
  count(distinct Cancellability)
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  Datasource = 'SYM'
group by
  policyid
having
  count(distinct Cancellability) >1
order by
  2 desc

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=758061

-- COMMAND ----------

-- MAGIC %md Step0: Step 0 Select policies where cancellability differs from zero

-- COMMAND ----------

select distinct PolicyId from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0

-- COMMAND ----------

-- MAGIC %md Step 1 Off each policy select the first contract and define a flag which is TRUE if cancellability equals zero

-- COMMAND ----------

select a.*,
case 
when Cancellability=0 then TRUE
Else FALSE
End AS CancellabilityZeroFlag
from MI2022.Contracts_SL1_20191231_v20220228_01 a where policyid in 
(select distinct PolicyId from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0 group by PolicyId) 


-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

drop view MI2022.Control3Init

-- COMMAND ----------

create view MI2022.Control3Init as 
select a.*,
case 
when Cancellability=0 then TRUE
Else FALSE
End AS CancellabilityZeroFlag
from MI2022.Contracts_SL1_20191231_v20220228_01 a where policyid in 
(select distinct PolicyId from MI2022.Contracts_SL1_20191231_v20220228_01 where Cancellability!=0 group by PolicyId)  and Cancellability!=0 and RiskPeriodEndDate > 20191231 and datasource='SYM'



-- COMMAND ----------

select count(*) from MI2022.Control3Init 

-- COMMAND ----------

select count(*) from MI2022.Control3Init 

-- COMMAND ----------

drop view MI2022.Control3

-- COMMAND ----------

Create view MI2022.Control3 as
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
  f.PreCreditPeriodTypePorceCode,
  f.PreCreditPeriodTypePormecode,
  f.PreCreditPeriodTypeProveCode,
  f.PreCreditPeriodTypeValue,
  f.PreCreditPeriodTypeLastUpdateDate,
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
  MI2022.Control3Init a
  left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 mod on a.PolicyId = mod.Bupiy_Id
  Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20191231_v20220228_01 refmodvar On (mod.porce_code = refmodvar.porce_code And mod.porme_Code = refmodvar.porme_code)
  Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 modvar On (refmodvar.porve_Code = modvar.porve_code And mod.bupiy_id = modVar.Bupiy_id)
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
    ,ctrygrp.max_credit_terms_per_typ) b on a.PolicyId=b.PolicyId
  left join (SELECT
    pol.bupiy_id as PolicyId,
    Max(ctrygrp.max_cred_per) as maximumCreditPeriod,
    ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
    FROM MI2022.Control3Init a left join
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
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id 
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    GROUP BY
    pol.bupiy_id, ctrygrp.max_credit_terms2_per_typ) d on a.PolicyId=d.PolicyId
  Left join (SELECT
    pol.bupiy_id as PolicyId,
    max(ctrygrp.waiting_period) as WaitingPeriod,
    ctrygrp.waiting_period_typ as WaitingPeriodType
    FROM MI2022.Control3Init a left join
    MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol on a.PolicyId=pol.bupiy_id
    JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20191231_v20220228_01 ctrygrp on ctrygrp.bupiy_id=pol.bupiy_id
    AND to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') BETWEEN to_date(ctrygrp.effect_from_dat, 'yyyyMMdd') AND to_date(ctrygrp.effect_to_dat, 'yyyyMMdd')
    Where Waiting_period <>0 
    GROUP BY
    pol.Bupiy_id,ctrygrp.waiting_period_typ) e on a.PolicyId=e.PolicyId
  Left join (Select
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
  and pvar.Effect_To_Dat>Current_Date()) f on a.PolicyId=f.PolicyId
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 i on a.policyid=i.id
Where 
mod.porce_Code = 'C00'
And mod.porme_code IN ('K001', 'K002')
And modvar.porve_code IN ('RAFLG', 'LOFLG')


-- COMMAND ----------

select distinct porve_code,value from MI2022.Control3

-- COMMAND ----------

select * from MI2022.Control3 where policyid=773208

-- COMMAND ----------

select * from MI2022.Control3 where policyid=773208

-- COMMAND ----------

select count(*) from MI2022.Control3

-- COMMAND ----------

select policyid,count(distinct Cancellability) from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM' group by policyid having count(distinct Cancellability)>1 order by 2 desc

-- COMMAND ----------

select * from  MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=754653
