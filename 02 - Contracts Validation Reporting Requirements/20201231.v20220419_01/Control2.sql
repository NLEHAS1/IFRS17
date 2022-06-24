-- Databricks notebook source


-- COMMAND ----------

Create view MI2022.Control2_20201231_v20220419_01 as
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
  MI2022.Contracts_SL1_20201231_v20220419_01
where 
  DataSource='SYM'
  and RiskPeriodEndDate>20201231
group by
  policyid,
  ContractIssueDate
having
  count(contractid) > 12) b
  left join MI2022.Contracts_SL1_20201231_v20220419_01 a on (b.PolicyId=a.PolicyId and b.ContractIssueDate=a.ContractIssueDate)
  left join (
    SELECT
      a.ID POLICY_ID,
      a.CMPCT_ID CMPCT_ID,
      a.d_popss_status_code POLICY_STATUS,
      b.PORME_CODE BREAK_MODULE_CODE,
      b.EFFECT_FROM_DAT EFFECTIVE_FROM,
      b.EFFECT_TO_DAT EFFECTIVE_TO
    FROM
      MI2022.TBBU_POLICIES_SL1_20201231_v20220419_01 a
      left join MI2022.TBPO_POL_MODULES_SL1_20201231_v20220419_01 b on a.ID = b.BUPIY_ID --AND d_popss_status_code in ('LIVE', 'SUSP')
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
  left join MI2022.TBPO_POL_VERSIONS_SL1_20201231_v20220419_01 d on (a.policyid = d.bupiy_id and to_date(d.start_risk_dat, 'yyyyMMdd') = to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd')) 
  left join  MI2022.TBPO_POL_MOD_VARIABLES_SL1_20201231_v20220419_01 c on (a.policyid=c.bupiy_id  and Current_Date() between c.effect_from_dat and c.effect_to_dat and c.porve_code IN ('RAFLG', 'LOFLG')) 
   --MaximumCreditPeriod  Based on Uli request on 1 April 2022
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            Max(ctrygrp.max_cred_per) as MaximumCreditPeriod,
            ctrygrp.max_cred_per_typ as MaximumCreditPeriodTermsType
            FROM MI2022.TBPO_POL_VERSIONS_SL1_20201231_v20220419_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_MCTS_SL1_20201231_v20220419_01 ctrygrp on (ctrygrp.bupiy_id=pol.Bupiy_id AND pol.end_risk_dat BETWEEN ctrygrp.effect_from_dat AND ctrygrp.effect_to_dat)
            where ctrygrp.max_cred_per is not null and ctrygrp.max_cred_per_typ is not null
            GROUP BY
            pol.bupiy_id,ctrygrp.max_cred_per_typ) d on a.PolicyId=d.PolicyId
    --WaitingPeriod Based on Uli request on 1 April 2022
  Left Join (SELECT
            pol.bupiy_id as PolicyId,
            max(ctrygrpcvr.waiting_period) as WaitingPeriod,
            ctrygrpcvr.waiting_period_typ as WaitingPeriodType
            FROM
            MI2022.TBPO_POL_VERSIONS_SL1_20201231_v20220419_01 pol
            JOIN MI2022.TBPO_CTRY_GRP_COVER_PCTS_SL1_20201231_v20220419_01 ctrygrpcvr on (
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
            From MI2022.TBBU_POLICIES_SL1_20201231_v20220419_01 a
            left join MI2022.TBPO_POL_MODULES_SL1_20201231_v20220419_01 pmod on pmod.bupiy_id=a.id
            Left  Join MI2022.TBPO_REF_MODULE_VARIABLE_TYPES_SL1_20201231_v20220419_01 mref
            On pmod.porme_code = mref.porme_code
            And pmod.porce_code = mref.Porce_code
            Left  Join MI2022.TBPO_POL_MOD_VARIABLES_SL1_20201231_v20220419_01 mvar
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
  and a.RiskPeriodEndDate > 20201231



-- COMMAND ----------

select count(*) from MI2022.Control2_20201231_v20220419_01
