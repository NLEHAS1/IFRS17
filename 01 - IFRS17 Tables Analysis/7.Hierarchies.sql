-- Databricks notebook source
show databases

-- COMMAND ----------

use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd

-- COMMAND ----------

-- MAGIC %md Create a Hierarchy table with Hierarchies levels

-- COMMAND ----------

select * from entities a left join hierarchies b on a.entityid=b.entityid

-- COMMAND ----------

select * from hierarchies

-- COMMAND ----------

drop view HierarchyLevel

-- COMMAND ----------

create view db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.HierarchyLevel as
seleCt
a.ValuationDate,
case
When a.EntityId=b.EntityId then "1"
when (a.EntityId!=b.EntityId and b.EntityId=c.EntityId) then "2"
when (a.EntityId!=b.EntityId and c.EntityId=d.EntityId) then "3"
When (a.EntityId!=b.EntityId and d.EntityId=e.EntityId) then "4"
Else "5"
End as HierarchyLevel,
a.EntityId,
a.HierarchyId,
a.ParentEntityId,
a.IFRS17CalculationEntity,
a.IFRS17ReportingEntity,
a.IFRS17ConsolidationEntity
from hierarchies a left join hierarchies b on a.ParentEntityId=b.EntityId left join hierarchies c on b.ParentEntityId=c.EntityId left join hierarchies d on c.ParentEntityId=d.EntityId left join hierarchies e on d.ParentEntityId=e.EntityId

-- COMMAND ----------

select * from db_2021_10_06_181520_non_cons_14pa_c0c56e3522820f74065a89e0ab369074baaaf905.HierarchyLevel

-- COMMAND ----------

select distinct CalculationEntity,HierarchyLevel from dcrhc a left join HierarchyLevel b on (a.CalculationEntity=b.EntityId)

-- COMMAND ----------

drop  view EntitiesHierarchies

-- COMMAND ----------

Create view EntitiesHierarchies as 
select a.EntityId as EntityL4,b.EntityId as  EntityL3,c.EntityId as EntityL2,c.ParentEntityId as  EntityL1 
from (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L4') a 
right join (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L3') b on a.ParentEntityId=b.EntityId 
right join (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L2') c on b.ParentEntityId=c.EntityId

-- COMMAND ----------

select * from EntitiesHierarchies

-- COMMAND ----------

create  view EntitiesHierarchiestable2 as
select
case
when EntityL4 is null then "0"
Else EntityL4
End as EntityL4,
case
when EntityL3 is null then "0"
Else EntityL3
End as EntityL3,
case
when EntityL2 is null then "0"
Else EntityL2
End as EntityL2,
case
when EntityL1 is null then "0"
Else EntityL1
End as EntityL1
from EntitiesHierarchies

-- COMMAND ----------

select * from EntitiesHierarchiestable2

-- COMMAND ----------

select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L4' 

-- COMMAND ----------

select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L3' 

-- COMMAND ----------

select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L2' 

-- COMMAND ----------

select a.EntityId as EntityL4,b.EntityId as  EntityL3,c.EntityId as EntityL2,c.ParentEntityId as  EntityL1 
from (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L4') a 
right join (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L3') b on a.ParentEntityId=b.EntityId 
right join (select EntityId,ParentEntityId from HierarchyLevel where HierarchyLevel='L2') c on b.ParentEntityId=c.EntityId

-- COMMAND ----------

select * from Contracts

-- COMMAND ----------

select * from cashflows

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select a.insurerid, b.InsurerId,b.entityid from contracts a left join entities b on a.insurerid=b.InsurerId 

-- COMMAND ----------

select * from hierarchies

-- COMMAND ----------

select * from HierarchyLevel
