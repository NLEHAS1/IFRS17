-- Databricks notebook source
-- MAGIC %md Step 0: Select policies with a PolicyGroupingId. 
-- MAGIC 
-- MAGIC In Contracts table policies where the ManagerdTogetherID differs from policyID are part of policy group with a GROUPING ID

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 where policyid!=ManagedTogetherId and datasource='SYM' order by ManagedTogetherId

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TBPO_POL_GROUP_POLICIES.POPGG_ID
-- MAGIC (group id of the policy)
-- MAGIC 
-- MAGIC https://confluence.atradius.com/confluence/display/IFRS/03T.CI.MOD.SYM.*.*+-+ManagedTogetherId

-- COMMAND ----------

select * from MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 where bupiy_id=177184

-- COMMAND ----------

select id,BUPIY_COINS_ID from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=167863

-- COMMAND ----------

select * from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=043110

-- COMMAND ----------

SELECT
to_char(TBMI_PARAMETERS.EXTRACT_DATE,'YYYYMM'),
A_WORKGRP_DEPTS.DEPT_NAME,
TBMI_POLICIES.ID,
TBMI_CUSTOMERS.ORNNN_ID,
TBMI_CUSTOMERS.D_ORONE_FIRST_LINE_NAME,
CONTACT_PERSON_FOR_POLGRP_HIST.D_ORIDL_INDIV_NAME,
POLICY_GROUPINGS_HIST.ID,
POL_GROUP_POLICIES_HIST.MASTER_FLAG,
BUNDLE_POLICIES_HIST.POBDE_SEQ,
BUNDLES_HIST.POBUT_TYP,
BUNDLES_HIST.MULTI_CUR_FLAG,
DECODE(NVL(BUNDLE_MAIN_POLICIES_HIST.POBDE_SEQ,0),0, 'N','Y'),
TBMI_POLICIES.D_POPSS_STATUS_CODE,
TBMI_POLICIES.D_POPSS_STATUS_CODE,
POLICY_GROUPINGS_HIST.EFFECT_FROM_DAT,
POLICY_GROUPINGS_HIST.EFFECT_TO_DAT,
POL_GROUP_POLICIES_HIST.EFFECT_FROM_DAT,
POL_GROUP_POLICIES_HIST.EFFECT_TO_DAT,
BUNDLES_HIST.EFFECT_FROM_DAT,
BUNDLES_HIST.EFFECT_TO_DAT,
BUNDLE_POLICIES_HIST.EFFECT_FROM_DAT,
BUNDLE_POLICIES_HIST.EFFECT_TO_DAT,
TBMI_POLICIES.D_POPVN_START_RISK_DAT,
TBMI_POLICIES.D_POPVN_END_RISK_DAT,
BUNDLE_TYPES_HIST.MAIN_POL_FLAG,
BUNDLE_TYPES_HIST.ALLOW_DIFF_END_DAT_FLAG
FROM
TBMI_CUSTOMERS,
TBMI_POLICIES,
TBWM_WORKGRP_DEPTS A_WORKGRP_DEPTS,  --found
TBMI_PARAMETERS,
TBOR_SYSTEM_USERS CONTACT_PERSON_FOR_POLGRP_HIST, --found
ORASTAG1.TBPO_POLICY_GROUPINGS POLICY_GROUPINGS_HIST,
ORASTAG1.TBPO_POL_GROUP_POLICIES POL_GROUP_POLICIES_HIST, --found
ORASTAG1.TBPO_BUNDLE_POLICIES BUNDLE_POLICIES_HIST, --found
ORASTAG1.TBPO_BUNDLES BUNDLES_HIST, --found
ORASTAG1.TBPO_BUNDLE_MAIN_POLICIES BUNDLE_MAIN_POLICIES_HIST, --found
ORASTAG1.TBPO_BUNDLE_TYPES BUNDLE_TYPES_HIST
WHERE
( TBMI_CUSTOMERS.ORNNN_ID=TBMI_POLICIES.ORCUR_ORNNN_ID )
AND ( TBMI_PARAMETERS.EXTRACT_DATE=TBMI_PARAMETERS.EXTRACT_DATE )
AND ( TBMI_PARAMETERS.BUCLT_ID<=TBMI_POLICIES.ID )
AND ( A_WORKGRP_DEPTS.ID(+)=TBMI_POLICIES.ORBUT_ID )
AND ( POL_GROUP_POLICIES_HIST.BUPIY_ID(+)=TBMI_POLICIES.ID )
AND ( POLICY_GROUPINGS_HIST.ID(+)=POL_GROUP_POLICIES_HIST.POPGG_ID )
AND ( CONTACT_PERSON_FOR_POLGRP_HIST.ID(+)=POLICY_GROUPINGS_HIST.ORSUS_MANAGER_ID )
AND ( BUNDLE_POLICIES_HIST.BUPIY_ID(+)=POL_GROUP_POLICIES_HIST.BUPIY_ID )
AND ( BUNDLES_HIST.SEQ(+)=BUNDLE_POLICIES_HIST.POBDE_SEQ )
AND ( BUNDLE_TYPES_HIST.TYP(+)=BUNDLES_HIST.POBUT_TYP )
AND ( BUNDLE_MAIN_POLICIES_HIST.BUPIY_ID(+)=BUNDLE_POLICIES_HIST.BUPIY_ID AND BUNDLE_MAIN_POLICIES_HIST.POBUT_TYP(+)=BUNDLE_POLICIES_HIST.POBUT_TYP AND BUNDLE_MAIN_POLICIES_HIST.POPGG_ID(+)=BUNDLE_POLICIES_HIST.POPGG_ID AND BUNDLE_MAIN_POLICIES_HIST.POBDE_SEQ(+)=BUNDLE_POLICIES_HIST.POBDE_SEQ )
AND ( BUNDLES_HIST.POPGG_ID(+)=BUNDLE_POLICIES_HIST.POPGG_ID )
AND ( BUNDLES_HIST.POBUT_TYP(+)=BUNDLE_POLICIES_HIST.POBUT_TYP )
AND
(
A_WORKGRP_DEPTS.DEPT_NAME IN ( 'ATFS AK Sigorta','ATFS TEB Sigorta','Austria','Austria Global','Austria SP','Austria SP Global','Czech','Czech Republic Global','Czech Republic SP','Czech Republic SP Global','GKS Germany','Germany Corporate','Germany Country','Germany Global','Germany SP','Germany SP Global','Global GKS Germany','Greece','Greece Global','Greece SP','Greece SP Global','Hungary','Hungary Global','Hungary SP','Hungary SP Global','MTB Germany','Poland','Poland Global','Poland SP','Poland SP Global','Slovakia','Slovakia Global','Slovakia SP','Slovakia SP Global','Switzerland','Switzerland Global','Switzerland SP','Switzerland SP Global','Turkey Global','Turkey Local','Atradius Bulgaria Country','Atradius Bulgaria Country SP','Atradius Bulgaria Global','Atradius Bulgaria Indexing','Atradius Romania Country','Atradius Romania Country SP','Atradius Romania Global','Atradius Romania Global SP','Atradius Romania Indexing' )
AND
TBMI_POLICIES.D_POPSS_STATUS_CODE IN ( 'LIVE','CANC','ANNU','SUSP' )
AND
TBMI_POLICIES.D_POPVN_END_RISK_DAT >= '01-01-2004 00:00:00'
AND
POLICY_GROUPINGS_HIST.ID Is Not Null
)

-- COMMAND ----------

-- MAGIC %md Based on Amir updates on 21 March 2022

-- COMMAND ----------

drop view MI2022.Control5

-- COMMAND ----------

create view MI2022.Control5 as
select
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id,
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether,
  b.d_popss_status_code
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 d on (
    a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd')
    and to_date(d.effect_to_dat, 'yyyyMMdd')
  ) --ContractIssueDate has been used to match between effect from and to date
where
  datasource = 'SYM'
  and a.RiskPeriodEndDate > 20191231

-- COMMAND ----------

select count(*) from MI2022.Control5

-- COMMAND ----------

select distinct d_popss_status_code from (
select
  a.*,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id, 
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether,
  b.d_popss_status_code
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 d on ( a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd') and to_date(d.effect_to_dat, 'yyyyMMdd'))  --ContractIssueDate has been used to match between effect from and to date
    where
  datasource = 'SYM'
)

-- COMMAND ----------

select * from (
select
  a.*,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id, 
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether,
  b.d_popss_status_code
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 d on ( a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd') and to_date(d.effect_to_dat, 'yyyyMMdd'))  --ContractIssueDate has been used to match between effect from and to date
    where
  datasource = 'SYM'
) where d_popss_status_code is null

-- COMMAND ----------

select id,d_popss_status_code from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 where id=58454

-- COMMAND ----------

select distinct d_popss_status_code from MI2022.Control5

-- COMMAND ----------

-- MAGIC %md ####New req from Richard 08-Apr-2022. The Code provided by Uli

-- COMMAND ----------

Select
    YYYYMM,
    Business_Unit,
    Policy_Id,
    Customer_or_Pol_Group_Id,
    Policy_Typ,
    Start_Risk_Date,
    End_Risk_Date,
    Closed_Dat,
    Status_Code,
    Exp_Prem_Total_EURO,
    Max_Invoice_YYYYMM,
    BS_PS_NCB,
    IML,
    AFL,
    PRIC
From
(
Select
    pol.JJJJMM_Reporting YYYYMM,
    pol.Business_Unit,
    pol.Policy_Id,
    pol.Customer_or_Pol_Group_Id,
    pol.Policy_Typ,
    pol.Start_Risk_Date,
    pol.End_Risk_Date,
    pol.Closed_Dat,
    Pol.Status_Code,
    pol.Exp_Prem_Total_EURO,
    Max(to_char(Invoice_Date,'YYYYMM')) Max_Invoice_YYYYMM,
    bundles.BS_PS_NCB,
    bundles.IML,
    bundles.AFL,
    bundles.PRIC
From DD_Pol_Hist pol                -- I don't have access to the table and the table not available 
Left Join FT_Invoices inv
On pol.Policy_id = inv.Policy_Id
Left  Join
(
Select
    Policy_Id,
    Policy_Group_Id,
    Start_Risk_Date,
    End_Risk_Date,
    max(Decode(Bundle_Type,'BS','BS')) "BS_PS_NCB",
    max(Decode(Bundle_Type,'IML','IML')) "IML",
    max(Decode(Bundle_Type,'AFL','AFL')) "AFL",
    max(Decode(Bundle_Type,'PRIC','PRIC')) "PRIC"      
From DD_Bundles   --tbpo_bundles
Where to_date('01.01.2020','dd.mm.yyyy') between Start_Risk_Date and End_Risk_Date
And to_date('01.01.2020','dd.mm.yyyy') between Bundle_Effect_From_Date and Bundle_Effect_To_Date
And to_date('01.01.2020','dd.mm.yyyy') between Bundle_Member_Effect_From_Date and Bundle_Member_Effect_To_Date
And Bundle_Type in ('BS', 'PS', 'NCB','IML','AFL','PRIC')
Group By     Policy_Id, Policy_Group_Id,
    Start_Risk_Date,
    End_Risk_Date
) bundles
on pol.Policy_Id = bundles.Policy_Id
Where pol.JJJJMM_Reporting =201912 --and pol.Policy_Id = 820033
And pol.Business_Unit not like '%Global%' And pol.Business_Unit not like '%SP%'
Group By JJJJMM_Reporting, pol.Business_Unit, pol.Policy_Id, pol.Customer_or_Pol_Group_Id, pol.Policy_Typ, pol.Start_Risk_Date, pol.End_Risk_Date,  pol.Closed_Dat, Pol.Status_Code, pol.Exp_Prem_Total_EURO,
    bundles.BS_PS_NCB,
    bundles.IML,
    bundles.AFL,
    bundles.PRIC
Order by pol.policy_Id
)

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20191231.v20220228_01/SourceData/CI")
