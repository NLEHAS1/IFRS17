-- Databricks notebook source
select id,BUPIY_COINS_ID from MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 -- BUPIY_COINS_ID:Indicates the master policy if the policy row represents a coinsured policy.

-- COMMAND ----------

select ManagedTogetherId, count (PolicyId) from MI2022.Contracts_SL1_20191231_v20220228_01 where datasource='SYM' group by ManagedTogetherId

-- COMMAND ----------

select * from MI2022.Contracts_SL1_20191231_v20220228_01 

-- COMMAND ----------

Richard: I expect that the non global policies, all the memebers of a group has the same mainunit as the group.

My translation: All the policies under one mangedtoghetherId should have the same mainunit, provided it's not global.

-- COMMAND ----------

select
  ManagedTogetherId,
  count(distinct mainunit)
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  ManagedTogetherId not in (select distinct ManagedTogetherId from MI2022.Contracts_SL1_20191231_v20220228_01 where MainUnit like 'GLB%' )
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
order by
  2 desc

-- COMMAND ----------

select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  DataSource='SYM' and RiskPeriodEndDate > 20191231
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
except
select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  mainunit like 'GLB%'

-- COMMAND ----------

select
  a.ManagedTogetherId,
  count(distinct a.mainunit)
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left anti join MI2022.Contracts_SL1_20191231_v20220228_01 b on (a.PolicyId=b.PolicyId and a.ContractId=b.ContractId and b.mainunit like 'GLB%' and a.DataSource='SYM' and b.DataSource='SYM' and a.RiskPeriodEndDate > 20191231 and b.RiskPeriodEndDate > 20191231) 
group by
   a.ManagedTogetherId
having
  count(distinct a.mainunit) > 1
order by
  2 desc

-- COMMAND ----------

select distinct ManagedTogetherId, mainunit, unit from MI2022.Contracts_SL1_20191231_v20220228_01 where ManagedTogetherId in (

select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  DataSource='SYM' and RiskPeriodEndDate > 20191231
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
except
select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  mainunit like 'GLB%'
)

-- COMMAND ----------

select * from  MI2022.Contracts_SL1_20191231_v20220228_01 where ManagedTogetherId=31873

-- COMMAND ----------

select * from MI2022.Control9Int where ManagedTogetherId=31873

-- COMMAND ----------

select * from MI2022.Control9 where ManagedTogetherId=31873

-- COMMAND ----------

-- MAGIC %md #Process

-- COMMAND ----------

-- MAGIC %md The code is ready, but validate your understanding with Uli before publish to PowerBi
-- MAGIC 
-- MAGIC My understanding: All the policies under one mangedtoghetherId should have the same mainunit, provided it's not global. So in this table I'm showing ManagedTogetherId wich have several mainunits

-- COMMAND ----------

drop view MI2022.Control9

-- COMMAND ----------

Create view MI2022.Control9Int as
select * from MI2022.Contracts_SL1_20191231_v20220228_01 where ManagedTogetherId in (select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  DataSource='SYM' and RiskPeriodEndDate > 20191231
group by
  ManagedTogetherId
having
  count(distinct mainunit) > 1
except
select
  ManagedTogetherId
from
  MI2022.Contracts_SL1_20191231_v20220228_01
where
  mainunit like 'GLB%') and DataSource='SYM'

-- COMMAND ----------

drop view MI2022.Control9 

-- COMMAND ----------

create view MI2022.Control9 as
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
  MI2022.Control9Int a
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 d on ( a.policyid = d.bupiy_id
    and to_date(cast(a.ContractIssueDate as string), 'yyyyMMdd') between to_date(d.effect_from_dat, 'yyyyMMdd') and to_date(d.effect_to_dat, 'yyyyMMdd'))  --ContractIssueDate has been used to match between effect from and to date

-- COMMAND ----------

select * from MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01

-- COMMAND ----------

select Effect_To_Dat from MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01

-- COMMAND ----------

select distinct datasource from MI2022.Control9

-- COMMAND ----------

select count(distinct ManagedTogetherId ) from Control9

-- COMMAND ----------

select * from MI2022.Control9

-- COMMAND ----------

select count(*) from MI2022.Control9

-- COMMAND ----------

select count(*) from MI2022.Control9

-- COMMAND ----------

select count(*) from MI2022.Control9

-- COMMAND ----------

select * from Control9 where mainunit like 'GLB%'

-- COMMAND ----------

select distinct unit,mainunit from Control9

-- COMMAND ----------

select count(*)  from Control9

-- COMMAND ----------

select *  from MI2022.Control9 where ManagedTogetherId=20417 

-- COMMAND ----------

select distinct mainunit from MI2022.Contracts_SL1_20191231_v20220228_01 where ManagedTogetherId=20417  

-- COMMAND ----------

select bupiy_id,count(*) from MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 where effect_from_dat>current_date() group by bupiy_id having count(*)>1 order by 2 desc

-- COMMAND ----------

select
  contractid, count(*)
from
  MI2022.Contracts_SL1_20191231_v20220228_01 a
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 b on (
    a.policyid = b.bupiy_id
    and to_date(cast(a.ContractIssueDate  as string), 'yyyyMMdd') between to_date(effect_from_dat, 'yyyyMMdd') and  to_date(effect_to_dat, 'yyyyMMdd')
  )
where
  a.datasource = 'SYM'
  group by 
  contractid
  having count(*)>1

-- COMMAND ----------

select * from  MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01  where bupiy_id=61388

-- COMMAND ----------

select distinct unit,mainunit from MI2022.Contracts_SL1_20191231_v20220228_01 

-- COMMAND ----------



-- COMMAND ----------

select distinct  ManagedTogetherId from MI2022.Control9Int 

-- COMMAND ----------

select
  a.*,
  b.ORCUR_ORNNN_ID as CustomerId,
  c.d_ornol_short_name as CustomerName,
  d.popgg_id, -- Based on Amir req
  Case
    when a.ManagedTogetherId = d.popgg_id then True
    Else False
  End as EqualPolicyGroupMangedTogether
from
  MI2022.Control9Int a
  left join MI2022.TBBU_POLICIES_SL1_20191231_v20220228_01 b on a.PolicyId = b.id
  Left join MI2022.TBOR_NON_NCM_ORGANISATIONS_SL1_20191231_v20220228_01 c on c.ID = b.ORCUR_ORNNN_ID
  and c.Effect_To_Dat > Current_Date()
  left join MI2022.TBPO_POL_GROUP_POLICIES_SL1_20191231_v20220228_01 d on ( a.policyid = d.bupiy_id
    and d.effect_to_dat>current_date()) 
