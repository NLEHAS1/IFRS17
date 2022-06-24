-- Databricks notebook source
drop view MI2022.Control9_20201231_v20220419_01

-- COMMAND ----------

create view MI2022.Control9_20191231_20220425_RUN127 as
with Control9Int (
select * from MI2022.Contracts_SL1_20191231_20220425_RUN127 where ManagedTogetherId in (select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_20220425_RUN127
where
  DataSource='SYM' and CoverEndDate > 20191231
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
except
select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_20220425_RUN127
where
  mainunit like 'GLB%') and DataSource='SYM'
)
select
  a.*,
  Left(a.ContractIssueDate, 4) as CohortYear,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id, -- Based on Amir req
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether
from
  Control9Int a
  left join MI2022.TBBU_POLICIES_SL1_20191231_20220425_RUN127 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_20220425_RUN127 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_20220425_RUN127 d on ( a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd') and to_date(d.effect_to_dat, 'yyyyMMdd'))  --ContractIssueDate has been used to match between effect from and to date

-- COMMAND ----------

select count(*) from MI2022.Control9_20191231_20220425_RUN127

-- COMMAND ----------

select count(*) from MI2022.Control9_20201231_v20220419_01

-- COMMAND ----------

select count(*) from MI2022.Control9_20201231_v20220419_01

-- COMMAND ----------

select riskperiodenddate from MI2022.Control9_20201231_v20220419_01 order by riskperiodenddate asc
