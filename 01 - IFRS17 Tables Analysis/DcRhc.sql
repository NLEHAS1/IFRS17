-- Databricks notebook source
use db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905;
show tables

-- COMMAND ----------

select * from dcrhc

-- COMMAND ----------

select * from dcrhc where CalculationEntity=001 and intra_group_elimination_flg is not null

-- COMMAND ----------

select * from dcrhc where contractid='SYM / 123864 / 20150601'

-- COMMAND ----------

select * from dcrhc where rein_contract_flg is null

-- COMMAND ----------

select GROUP_TYPE_CD,ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity,direct_contract_flg,rein_contract_flg,count(*) from dcrhc group by GROUP_TYPE_CD,ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity,direct_contract_flg,rein_contract_flg having count(*)>1

-- COMMAND ----------

select GROUP_TYPE_CD,ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity,count(*) from dcrhc group by GROUP_TYPE_CD,ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity having count(*)>1

-- COMMAND ----------

select ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity,count(*) from dcrhc where GROUP_TYPE_CD='SUBGROUP' group by ContractId,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity having count(*)>1

-- COMMAND ----------

select * from dcrhc where contractid='SYM / 577104 / 20150101' and MAIN_INSURANCE_CONTRACT_GROUP_ID='CI-MTB_GLB#RM#2015' and CalculationEntity='D35'

-- COMMAND ----------

select count(distinct contractid) from dcrhc where CalculationEntity=207 -- and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select ContractId,
CalculationEntity,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg,
count(*)
from dcrhc
group by
ContractId,
CalculationEntity,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
having count(*)>1
order by 6 desc

-- COMMAND ----------

select * from dcrhc where contractid='SYM / 984781 / 20181001' and CalculationEntity=015 and intra_group_elimination_flg is null and direct_contract_flg=1 and rein_contract_flg=0

-- COMMAND ----------

select distinct CalculationEntity from dcrhc

-- COMMAND ----------

select count(*) from dcrhc

-- COMMAND ----------

select count(distinct contractid) from dcrhc

-- COMMAND ----------

select count(distinct contractid) from oneview

-- COMMAND ----------

select contractid,count(*) from dcrhc where GROUP_TYPE_CD='SUBGROUP' group by ContractId having count(*) =

-- COMMAND ----------

select contractid,count(*) from dcrhc where GROUP_TYPE_CD='SUBGROUP' and intra_group_elimination_flg is null group by ContractId order by 2 desc limit 500000



-- COMMAND ----------

select avg(count) from (select contractid,count(*) as count from dcrhc where GROUP_TYPE_CD='SUBGROUP' and intra_group_elimination_flg is null group by ContractId )


-- COMMAND ----------

select avg(count) from (select contractid,count(*) as count from oneview where intra_group_elimination_flg is null group by ContractId )


-- COMMAND ----------

select contractid,count(*) from DCrhcModeified where intra_group_elimination_flg=0 group by ContractId order by 2 desc 

-- COMMAND ----------

select * from dcrhc where contractid='1911@-1@15381551@15760182' and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select * from dcrhc where contractid='SYM / 468106 / 19990501' and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select * from dcrhc where ContractId='1911@-1@15381551@15760182' and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select * from cashflows where ContractId='1911@-1@15381551@15760182'

-- COMMAND ----------

select * from DCrhcModeified where ContractId='1911@-1@15381551@15760182'

-- COMMAND ----------

select * from Signage  where contractid='SYM / 468106 / 19990501' and cashflowId='InvoicedPremium#559656#SYM / 468106 / 19990501#PRP' order by intra_group_elimination_flg,	direct_contract_flg, rein_contract_flg desc

-- COMMAND ----------

drop view tmp666

-- COMMAND ----------

select contractid,CalculationEntity, 
HierarchyLevel,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc a left join HierarchyLevel b on (a.CalculationEntity=b.Entityid) where GROUP_TYPE_CD='SUBGROUP' and CalculationEntity=411

-- COMMAND ----------

Create view tmp666 as 
select contractid,CalculationEntity, 
case
when CalculationEntity='001' then "L1"
When CalculationEntity='015' then "L2"
When CalculationEntity in ('203','285','019','D35','013','411') then "L3"
else "L4"
End as Hierarchy,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc where GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select * from tmp666

-- COMMAND ----------

drop view temp6666

-- COMMAND ----------

create view temp6666 as 
select * from tmp666
  PIVOT ( max(CalculationEntity) for Hierarchy in ('L1','L2','L3','L4')) 



-- COMMAND ----------

select * from temp6666

-- COMMAND ----------

drop view temp111

-- COMMAND ----------

create view temp111 as
select distinct a.ContractId,b.INSURANCE_CONTRACT_GROUP_ID,b.MAIN_INSURANCE_CONTRACT_GROUP_ID, a.intra_group_elimination_flg,a.direct_contract_flg,a.rein_contract_flg,L1,L2,L3,L4 from temp6666 a left join dcrhc b on (a.contractid=b.contractid  and  a.direct_contract_flg=b.direct_contract_flg and a.rein_contract_flg=b.rein_contract_flg and GROUP_TYPE_CD='SUBGROUP') 

-- COMMAND ----------

select count(*) from temp111

-- COMMAND ----------

select count(*) from dcrhc

-- COMMAND ----------

select *  from temp111

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md Assimble the views

-- COMMAND ----------

Create view tmp666 as 
select contractid,CalculationEntity, 
case
when CalculationEntity='001' then "L1"
When CalculationEntity='015' then "L2"
When CalculationEntity in ('203','285','019','D35','013','411') then "L3"
else "L4"
End as Hierarchy,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc where GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

create view temp6666 as 
select * from tmp666
  PIVOT ( max(CalculationEntity) for Hierarchy in ('L1','L2','L3','L4')) 



-- COMMAND ----------

create view temp111 as
select distinct a.ContractId,b.INSURANCE_CONTRACT_GROUP_ID,b.MAIN_INSURANCE_CONTRACT_GROUP_ID, a.intra_group_elimination_flg,a.direct_contract_flg,a.rein_contract_flg,L1,L2,L3,L4 from temp6666 a left join dcrhc b on (a.contractid=b.contractid  and  a.direct_contract_flg=b.direct_contract_flg and a.rein_contract_flg=b.rein_contract_flg and GROUP_TYPE_CD='SUBGROUP') 

-- COMMAND ----------

-- MAGIC %md Create one View

-- COMMAND ----------

drop view oneview

-- COMMAND ----------

-- MAGIC %md new OneView with Hierarchy level already implemnted without hard coding

-- COMMAND ----------

-- MAGIC %md the view with Intermidary view. 

-- COMMAND ----------

Create view DCrhcModeified as
select
  distinct a.ContractId,
  b.INSURANCE_CONTRACT_GROUP_ID,
  b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
  a.intra_group_elimination_flg,
  a.direct_contract_flg,
  a.rein_contract_flg,
  L1,
  L2,
  L3,
  L4
from
  (select * from ( select contractid,CalculationEntity, 
HierarchyLevel,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc a left join HierarchyLevel b on (a.CalculationEntity=b.Entityid) where GROUP_TYPE_CD='SUBGROUP') 
          PIVOT (
            max(CalculationEntity) for HierarchyLevel in ('L1', 'L2', 'L3', 'L4','L5'))) a
  left join dcrhc b on (
    a.contractid = b.contractid
    and a.direct_contract_flg = b.direct_contract_flg
    and a.rein_contract_flg = b.rein_contract_flg
    and GROUP_TYPE_CD = 'SUBGROUP')

-- COMMAND ----------

-- MAGIC %md the view without Intermidary view

-- COMMAND ----------

drop view DCrhcModeified

-- COMMAND ----------

-- MAGIC %md the one I'm using for signage2 and 1 and signage intial 

-- COMMAND ----------

Create view DCrhcModeified as
select
  distinct a.ContractId,
  b.INSURANCE_CONTRACT_GROUP_ID,
  b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
  Case
  When a.intra_group_elimination_flg is null then 0
  else a.intra_group_elimination_flg
  End as intra_group_elimination_flg,
  a.direct_contract_flg,
  a.rein_contract_flg,
  case
  when CalculationEntityL1 is null then "None"
  Else CalculationEntityL1
  End as CalculationEntityL1,
  case
  when CalculationEntityL2 is null then "None"
  Else CalculationEntityL2
  End as CalculationEntityL2,
  case
  when CalculationEntityL3 is null then "None"
  Else CalculationEntityL3
  End as CalculationEntityL3,
  case
  when CalculationEntityL4 is null then "None"
  Else CalculationEntityL4
  End as CalculationEntityL4,
  case
  when CalculationEntityL5 is null then "None"
  Else CalculationEntityL5
  End as CalculationEntityL5
from
  (select * from ( select contractid,CalculationEntity, 
HierarchyLevel,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc a left join (seleCt
a.ValuationDate,
case
When a.EntityId=b.EntityId then "CalculationEntityL1"
when (a.EntityId!=b.EntityId and b.EntityId=c.EntityId) then "CalculationEntityL2"
when (a.EntityId!=b.EntityId and c.EntityId=d.EntityId) then "CalculationEntityL3"
When (a.EntityId!=b.EntityId and d.EntityId=e.EntityId) then "CalculationEntityL4"
Else "CalculationEntityL5"
End as HierarchyLevel,
a.EntityId,
a.HierarchyId,
a.ParentEntityId,
a.IFRS17CalculationEntity,
a.IFRS17ReportingEntity,
a.IFRS17ConsolidationEntity
from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId) b on (a.CalculationEntity=b.Entityid) where GROUP_TYPE_CD='SUBGROUP') 
          PIVOT (
            max(CalculationEntity) for HierarchyLevel in ('CalculationEntityL1', 'CalculationEntityL2', 'CalculationEntityL3', 'CalculationEntityL4','CalculationEntityL5'))) a
  left join dcrhc b on (
    a.contractid = b.contractid
    and a.direct_contract_flg = b.direct_contract_flg
    and a.rein_contract_flg = b.rein_contract_flg
    and GROUP_TYPE_CD = 'SUBGROUP')

-- COMMAND ----------

select * from DCrhcModeified

-- COMMAND ----------

select * from DCrhcModeified 
except 
select * from temp111

-- COMMAND ----------

select * from temp111
except 
select * from DCrhcModeified

-- COMMAND ----------

select count(*) from oneview --new

-- COMMAND ----------

select count(*) from oneview --old

-- COMMAND ----------

select count(*) from temp111

-- COMMAND ----------

-- MAGIC %md ####Validating OneView/Temp111

-- COMMAND ----------

-- MAGIC %md As we can see where L3 and L4 is null we have only Intra Group Contracts so my code to include L3 and L4 in the calculation is sound

-- COMMAND ----------

select * from OneView where L3 is null and L4 is null

-- COMMAND ----------

select * from OneView where L1 is null and L2 is not null and L3 is null and L4 is null

-- COMMAND ----------

select * from OneView where L1 is not null and L2 is  null and L3 is null and L4 is null

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from dcrhc where contractid='SYM / 211804 / 20180801'

-- SYM / 211804 / 20190501
-- SYM / 211804 / 20180801

-- COMMAND ----------

select distinct contractid, count(distinct intra_group_elimination_flg, direct_contract_flg, rein_contract_flg) from dcrhc group by contractid order by 2 desc

-- COMMAND ----------

select * from entities where insurerid=18877258

-- COMMAND ----------

 select * from DCRHC where contractid='1911@-1@15381551@15760182' and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select * from db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.hierarchies where entityid=441

-- COMMAND ----------

select * from hierarchies where entityid='001'

-- COMMAND ----------

select distinct ParentEntityId from hierarchies

-- COMMAND ----------

seleCt
case
When a.EntityId=b.EntityId then "L1"
when (a.EntityId!=b.EntityId and b.EntityId=c.EntityId) then "L2"
when (a.EntityId!=b.EntityId and c.EntityId=d.EntityId) then "L3"
When (a.EntityId!=b.EntityId and d.EntityId=e.EntityId) then "L4"
Else "L5"
End as levels,
a.EntityId,
a.ParentEntityId
from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId

-- COMMAND ----------

--create view hierarchiesfull as
select * from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId where a.entityid in ('491','494')

-- COMMAND ----------

--create view hierarchiesfull as
select * from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId where a.entityid=441

-- COMMAND ----------

select * from hierarchies where entityid=441 

-- COMMAND ----------

select * from hierarchies where entityid=411

-- COMMAND ----------

Create view DCrhcModeified as
select
  distinct a.ContractId,
  b.INSURANCE_CONTRACT_GROUP_ID,
  b.MAIN_INSURANCE_CONTRACT_GROUP_ID,
  Case
  When a.intra_group_elimination_flg is null then 0
  else a.intra_group_elimination_flg
  End as intra_group_elimination_flg,
  a.direct_contract_flg,
  a.rein_contract_flg,
  case
  when CalculationEntityL1 is null then "None"
  Else CalculationEntityL1
  End as CalculationEntityL1,
  case
  when CalculationEntityL2 is null then "None"
  Else CalculationEntityL2
  End as CalculationEntityL2,
  case
  when CalculationEntityL3 is null then "None"
  Else CalculationEntityL3
  End as CalculationEntityL3,
  case
  when CalculationEntityL4 is null then "None"
  Else CalculationEntityL4
  End as CalculationEntityL4,
  case
  when CalculationEntityL5 is null then "None"
  Else CalculationEntityL5
  End as CalculationEntityL5
from
  (select * from ( select contractid,CalculationEntity, 
HierarchyLevel,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from dcrhc a left join (seleCt
a.ValuationDate,
case
When a.EntityId=b.EntityId then "CalculationEntityL1"
when (a.EntityId!=b.EntityId and b.EntityId=c.EntityId) then "CalculationEntityL2"
when (a.EntityId!=b.EntityId and c.EntityId=d.EntityId) then "CalculationEntityL3"
When (a.EntityId!=b.EntityId and d.EntityId=e.EntityId) then "CalculationEntityL4"
Else "CalculationEntityL5"
End as HierarchyLevel,
a.EntityId,
a.HierarchyId,
a.ParentEntityId,
a.IFRS17CalculationEntity,
a.IFRS17ReportingEntity,
a.IFRS17ConsolidationEntity
from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId) b on (a.CalculationEntity=b.Entityid) where GROUP_TYPE_CD='SUBGROUP') 
          PIVOT (
            max(CalculationEntity) for HierarchyLevel in ('CalculationEntityL1', 'CalculationEntityL2', 'CalculationEntityL3', 'CalculationEntityL4','CalculationEntityL5'))) a
  left join dcrhc b on (
    a.contractid = b.contractid
    and a.direct_contract_flg = b.direct_contract_flg
    and a.rein_contract_flg = b.rein_contract_flg
    and GROUP_TYPE_CD = 'SUBGROUP')

-- COMMAND ----------

select * from dcrhc

-- COMMAND ----------

drop view DCrhcModeified2

-- COMMAND ----------

Create view DCrhcModeified2 as
select distinct INSURANCE_CONTRACT_GROUP_ID,MAIN_INSURANCE_CONTRACT_GROUP_ID,ContractId,CalculationEntity,HierarchyLevel,
 Case
  When intra_group_elimination_flg is null then 0
  else intra_group_elimination_flg
  End as intra_group_elimination_flg,
  direct_contract_flg,
  rein_contract_flg
from dcrhc left join HierarchyLevel on CalculationEntity=EntityId where intra_group_elimination_flg is null and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select* from DCrhcModeified2

-- COMMAND ----------

SYM / 225158 / 20150101
2094@-1@17299963@15319619


-- COMMAND ----------

-- MAGIC %md The code below extract the Lowest (max) level of the calculation entities. I have to find a way to combine it with DCrhcModeified2

-- COMMAND ----------

with cte as (
select *,row_number() over  (partition by contractid order by HierarchyLevel desc ) as rn from (select distinct INSURANCE_CONTRACT_GROUP_ID,MAIN_INSURANCE_CONTRACT_GROUP_ID,ContractId,CalculationEntity,HierarchyLevel,
 Case
  When intra_group_elimination_flg is null then 0
  else intra_group_elimination_flg
  End as intra_group_elimination_flg,
  direct_contract_flg,
  rein_contract_flg
from dcrhc left join HierarchyLevel on CalculationEntity=EntityId where intra_group_elimination_flg is null and GROUP_TYPE_CD='SUBGROUP')
)
select Contractid,CalculationEntity from cte where rn = 1 

-- COMMAND ----------

select * from DCrhcModeified2 where ContractId='SYM / 225158 / 20150101' -- and min(HierarchyLevel)

-- COMMAND ----------

select count(*) from DCrhcModeified2

-- COMMAND ----------

select * from dcrhc where ContractId='2094@-1@17299963@15319619' and GROUP_TYPE_CD='SUBGROUP'

-- COMMAND ----------

select count(*) from dcrhc where GROUP_TYPE_CD='SUBGROUP' and intra_group_elimination_flg is null

-- COMMAND ----------

select * from dcrhc

-- COMMAND ----------

select count(*) from dcrhc

-- COMMAND ----------

select count(distinct ContractId, INSURANCE_CONTRACT_GROUP_ID,MAIN_INSURANCE_CONTRACT_GROUP_ID,CalculationEntity ) from dcrhc

-- COMMAND ----------

-- MAGIC %md The code below show us the number of distinct contractid at each Claculation entity hierarchy

-- COMMAND ----------

select CalculationEntityL1,CalculationEntityL2,CalculationEntityL3,CalculationEntityL4,CalculationEntityL5,count(distinct ContractId) as Value from DCrhcModeified 
group by CalculationEntityL1,CalculationEntityL2,CalculationEntityL3,CalculationEntityL4,CalculationEntityL5
order by value desc

-- COMMAND ----------


