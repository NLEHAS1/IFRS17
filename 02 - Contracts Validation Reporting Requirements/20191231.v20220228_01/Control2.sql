-- Databricks notebook source
-- MAGIC %md Step 0 and step 1: Select policies where cohort contains more than 12 contracts
-- MAGIC 
-- MAGIC Count for each policy version the number of contracts having the same ContractIssueDate 
-- MAGIC 
-- MAGIC List the policies with more than 12 contracts and aske unit to check on 1) correctness of data and 2) that multiyear policies with break clause are not included in this list

-- COMMAND ----------

select
  policyid,
  ContractIssueDate,
  count(contractid)
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where 
  DataSource='SYM'
  and RiskPeriodEndDate>20191231
group by
  policyid,
  ContractIssueDate
having
  count(contractid) > 12
order by
  3 desc

-- COMMAND ----------



-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid= 56920 and ContractIssueDate=19980701

-- COMMAND ----------

-- MAGIC %md Step2: list the policies with more than 12 contracts and aske unit to check on 
-- MAGIC 1) correctness of data and 
-- MAGIC 2) that multiyear policies with break clause are not included in this 
-- MAGIC 
-- MAGIC So only list policy versions where count 13 or larger. Multiyear policies with a break clause should not be in the list

-- COMMAND ----------

select policyid ,count(contractid) from MI2022.Contracts_SL1_20191231_v20220228_01 group by policyid having count(contractid)>12

-- COMMAND ----------

-- MAGIC %md Step 3: Add a flag, whether policy has break clause (SCD or data team to provide how) 

-- COMMAND ----------

SELECT
  a.ID POLICY_ID,
  a.CMPCT_ID CMPCT_ID,
  a.d_popss_status_code POLICY_STATUS,
  b.PORME_CODE BREAK_MODULE_CODE,
  b.EFFECT_FROM_DAT EFFECTIVE_FROM,
  b.EFFECT_TO_DAT EFFECTIVE_TO
FROM
  MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a,
  MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 b
WHERE
  a.ID = b.BUPIY_ID
  AND d_popss_status_code in ('LIVE', 'SUSP')
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

select policy_id,count(*) from (
SELECT
  a.ID POLICY_ID,
  a.CMPCT_ID CMPCT_ID,
  a.d_popss_status_code POLICY_STATUS,
  b.PORME_CODE BREAK_MODULE_CODE,
  b.EFFECT_FROM_DAT EFFECTIVE_FROM,
  b.EFFECT_TO_DAT EFFECTIVE_TO
FROM
  MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a,
  MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 b
WHERE
  a.ID = b.BUPIY_ID
  AND d_popss_status_code in ('LIVE', 'SUSP')
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
  and b.Effect_To_Dat>Current_Date()
order by
  a.ID) group by policy_id having count(*)>1 order by 2 desc

-- COMMAND ----------

SELECT
  a.ID POLICY_ID,
  a.CMPCT_ID CMPCT_ID,
  a.d_popss_status_code POLICY_STATUS,
  b.PORME_CODE BREAK_MODULE_CODE,
  b.EFFECT_FROM_DAT EFFECTIVE_FROM,
  b.EFFECT_TO_DAT EFFECTIVE_TO
FROM
  MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a,
  MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 b
WHERE
  a.ID = b.BUPIY_ID
  AND d_popss_status_code in ('LIVE', 'SUSP')
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
  and b.Effect_To_Dat>Current_Date()
  and a.id=368984
order by
  a.ID

-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

drop view MI2022.Control2

-- COMMAND ----------

Create view MI2022.Control2 as
Select
distinct
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.CMPCT_ID,
  b.POLICY_STATUS,
  b.BREAK_MODULE_CODE,
  b.EFFECTIVE_FROM,
  b.EFFECTIVE_TO,
  d.start_risk_dat as PolicyStartDate, --->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskPeriodStartDate  ---> Amir request
  d.end_risk_dat as PolicyEndDate, -->https://confluence.atradius.com/confluence/display/IFRS/03.CI.MOD.SYM.*.*+-+RiskAttachmentEndDate    ---> Amir request
  c.unicode_value as Variable_Value,
  d.MaximumCreditPeriod,
  d.MaximumCreditPeriodTermsType,
  f.PreCreditPeriodType_EffectFromDate,
  f.PreCreditPeriodType_EffectToDate,
  f.PreCreditPeriodType_PorceCode,
  f.PreCreditPeriodTypePormeCode,
  f.MaxCrd_Typ as Max_PCR_Period_Type,
  f.MaxCrd_Value as Max_PCR_Period,
  f.MaxCrd_PerfDat,
  f.SelPCRCover as Selective_PCR_cover,
  e.WaitingPeriod,
  e.WaitingPeriodType
from
  (select
  policyid,
  ContractIssueDate,
  count(contractid)
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where 
  DataSource='SYM'
  and RiskPeriodEndDate>20191231
group by
  policyid,
  ContractIssueDate
having
  count(contractid) > 12) b
  left join MI2022.Contracts_SL1_20191231_v20220228_01 a on (b.PolicyId=a.PolicyId and b.ContractIssueDate=a.ContractIssueDate)
  left join (
    SELECT
      a.ID POLICY_ID,
      a.CMPCT_ID CMPCT_ID,
      a.d_popss_status_code POLICY_STATUS,
      b.PORME_CODE BREAK_MODULE_CODE,
      b.EFFECT_FROM_DAT EFFECTIVE_FROM,
      b.EFFECT_TO_DAT EFFECTIVE_TO
    FROM
      MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
      left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 b on a.ID = b.BUPIY_ID --AND d_popss_status_code in ('LIVE', 'SUSP')
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
      and b.Effect_To_Dat > Current_Date()
  ) b on a.PolicyId = b.POLICY_ID
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 d on (a.policyid = d.bupiy_id and to_date(d.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
  left join  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20191231_v20220228_01 c on (a.policyid=c.bupiy_id  and Current_Date() between c.effect_from_dat and c.effect_to_dat and c.porve_code IN ('RAFLG', 'LOFLG')) 
   --MaximumCreditPeriod  Based on Uli request on 1 April 2022
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
            ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20191231_v20220228_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_cred_per_typ) d on a.PolicyId=d.PolicyId
    --WaitingPeriod Based on Uli request on 1 April 2022
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
    --PreCreditPeriodType Based on Uli request on 1 April 2022
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
where
  datasource = 'SYM'
  and a.RiskPeriodEndDate > 20191231



-- COMMAND ----------

select * from MI2022.Control2  where mainunit='LOC-AUS'

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM'

-- COMMAND ----------

select count(*) from MI2022.Control2

-- COMMAND ----------

select * from MI2022.Control2 

-- COMMAND ----------

select distinct d_popss_status_code from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.policyid=b.id

-- COMMAND ----------

select policyid,ContractIssueDate,count(contractid) from MI2022.Control2 WHERE BREAK_MODULE_CODE IS NOT NULL group by policyid,ContractIssueDate having count(contractid)>12 order by 3 desc

-- COMMAND ----------

select policyid, ContractIssueDate,count(contractid) as CountOfContractId from (
Select
  a.*,
  b.CMPCT_ID,
  b.POLICY_STATUS,
  b.BREAK_MODULE_CODE,
  b.EFFECTIVE_FROM,
  b.EFFECTIVE_TO
from
  (select
  policyid,
  ContractIssueDate,
  count(contractid)
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where 
  DataSource='SYM'
  and RiskPeriodEndDate>20191231
group by
  policyid,
  ContractIssueDate
having
  count(contractid) > 12) b
  left join MI2022.Contracts_SL1_20191231_v20220228_01 a on (b.PolicyId=a.PolicyId and b.ContractIssueDate=a.ContractIssueDate)
  left join (
    SELECT
      a.ID POLICY_ID,
      a.CMPCT_ID CMPCT_ID,
      a.d_popss_status_code POLICY_STATUS,
      b.PORME_CODE BREAK_MODULE_CODE,
      b.EFFECT_FROM_DAT EFFECTIVE_FROM,
      b.EFFECT_TO_DAT EFFECTIVE_TO
    FROM
      MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 a
      left join MI2022.TBPO_POL_MODULES_SL1_20191231_v20220228_01 b on a.ID = b.BUPIY_ID --AND d_popss_status_code in ('LIVE', 'SUSP')
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
      and b.Effect_To_Dat > Current_Date()
  ) b on a.PolicyId = b.POLICY_ID
where
  datasource = 'SYM'
  and a.RiskPeriodEndDate > 20191231)
  
  group by policyid, ContractIssueDate
  order by CountOfContractId asc


-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=116451

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=116451

-- COMMAND ----------

select
  policyid,ContractIssueDate,start_risk_dat,end_risk_dat
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 d on (a.policyid = d.bupiy_id
  and to_date(d.start_risk_dat, 'yyyyMMdd') = to_date(
    cast(a.ContractIssueDate as string), 'yyyyMMdd')) where policyid=116451

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM'  and policyid=478949

-- COMMAND ----------

select distinct producttype from MI2022.Contracts_SL1_20191231_v20220228_01 where mainproduct='CI-ST'

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=478949

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM'  and policyid=478949

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 b on a.policyid=b.bupiy_id where policyid=478949 and to_date(cast(CoverStartDate as string), 'yyyyMMdd') and to_date(cast(CoverEndDate as string), 'yyyyMMdd') between to_date(start_risk_dat,'yyyyMMdd') and to_date(end_risk_dat,'yyyyMMdd')

-- COMMAND ----------

select
  CoverStartDate,CoverEndDate
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 b on a.policyid = b.bupiy_id
where
  policyid = 478949
  and to_date(cast(CoverStartDate as string), 'yyyyMMdd') >= to_date(start_risk_dat, 'yyyyMMdd')
  and to_date(cast(CoverEndDate as string), 'yyyyMMdd') <= to_date(end_risk_dat, 'yyyyMMdd')

-- COMMAND ----------

select * from  MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=478949

-- COMMAND ----------

select *  from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=478949 order by CoverStartDate asc

-- COMMAND ----------

-- MAGIC %md here

-- COMMAND ----------

select distinct policyid,start_risk_dat, dense_rank() OVER( ORDER BY  bupiy_id,start_risk_dat) as PolicyVersion
from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (a.policyid = g.bupiy_id and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd'))  where policyid=478949




-- COMMAND ----------

drop view MI2022.PolicyVersion

-- COMMAND ----------

Create view MI2022.PolicyVersion as
select
  distinct policyid,
  start_risk_dat,
  dense_rank() OVER (
    PARTITION BY bupiy_id
    ORDER BY
      start_risk_dat
  ) as PolicyVersion
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (
    a.policyid = g.bupiy_id
    and to_date(g.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')
  )
where
  datasource = 'SYM'
  and RiskPeriodEndDate > 20191231
order by
  policyid

-- COMMAND ----------

select * from MI2022.PolicyVersion where PolicyVersion=2

-- COMMAND ----------

select distinct PolicyVersion from MI2022.PolicyVersion 

-- COMMAND ----------

select * from MI2022.PolicyVersion where policyid=478949

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=376887

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where   policyid=376887 and riskperiodenddate>20191231

-- COMMAND ----------

select d_popvn_start_risk_dat, d_popvn_end_risk_dat from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=1000522

-- COMMAND ----------

select PolicyId,start_risk_dat,ContractIssueDate
from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 g on (a.policyid = g.bupiy_id) where 
to_date(cast(ContractIssueDate as string), 'yyyyMMdd') != to_date(start_risk_dat, 'yyyyMMdd') and datasource='SYM' and RiskPeriodEndDate>20191231 

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where   policyid=478940 and riskperiodenddate>20191231

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=478940

-- COMMAND ----------

drop view MI2022.PolicyVersion

-- COMMAND ----------

Create view MI2022.PolicyVersion2 as
select
  policyid,
  ContractIssueDate,
  dense_rank() OVER (
    PARTITION BY policyid
    ORDER BY
      ContractIssueDate
  ) as PolicyVersion
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
where
  datasource = 'SYM'
  and RiskPeriodEndDate > 20191231
order by
  policyid

-- COMMAND ----------

select distinct PolicyVersion from MI2022.PolicyVersion2

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=1000123

-- COMMAND ----------

select policyid,ContractIssueDate,RiskPeriodStartDate,RiskPeriodEndDate from MI2022.Contracts_SL1_20191231_v20220228_01  where policyid=1000123 and RiskPeriodEndDate > 20191231

-- COMMAND ----------

select policyid,ContractIssueDate,RiskPeriodStartDate,RiskPeriodEndDate,start_risk_dat,end_risk_dat from MI2022.Contracts_SL1_20191231_v20220228_01 a left join MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 d on (a.policyid = d.bupiy_id and to_date(d.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd'))  where policyid=1000123 and RiskPeriodEndDate > 20191231

-- COMMAND ----------

-- MAGIC %md I can't find ContractIssueDate 20190215 for contracts with RiskPerioEndDate>20191231 because they have less then 12 contracts (check the view)

-- COMMAND ----------

select * from MI2022.Control2 where policyid=1000123 

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=902007 

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid=902007 and riskperiodenddate>20191231

-- COMMAND ----------

select * from MI2022.Control2 where policyid=1028594

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
            pivot (max(PreCreditPeriodTypeValue) for PreCreditPeriodTypePorveCode in 
            ('PERTYP3' as MaxCrd_Typ ,'MAXPRCR' as MaxCrd_Value,'PERFDAT' as MaxCrd_PerfDat,'SELPCRCO' as SelPCRCover)) where PolicyId=1028594
