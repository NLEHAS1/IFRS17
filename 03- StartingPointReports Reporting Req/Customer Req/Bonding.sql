-- Databricks notebook source
-- MAGIC %md What I understood from Simone is the following:
-- MAGIC 
-- MAGIC 1. InsuredId is the customer and to obtain the CustomerName we can use the InsuredId to extract CustomerName from Symphony Customer Table
-- MAGIC 2. If we still need to extract the CustomerId we can do for each case explained below
-- MAGIC 3. Some cases the InsuredId begin like this BEY@ or NAV@ and then with CustomerId then we ave to go back to the CustomerId in those system to extract the CustomerName

-- COMMAND ----------

-- MAGIC %md #BEY

-- COMMAND ----------

-- MAGIC %md We have duplicates

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON

-- COMMAND ----------


  select
    case
      When InsuredId like 'BEY@%' then split(InsuredId, '@') [1]
      Else InsuredId
    End as CustomerId,
    Case
      When InsuredId like 'BEY@%' then ragionesociale
      Else d_ornol_short_name
    End as CustomerName,
    a.*
  from
    db_202012_20220610_000047_502.contracts a
    left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (
      InsuredId = Id
      and b.effect_to_dat > current_date()
    )
    left join sl1_20201231_v20220529_03.ALLCURPOCA_CON c on (a.PolicyId=c.npolizza )  inner join sl1_20201231_v20220529_03.ANAGCC_CON d on (c.nposizione=d.nposizione
    and to_date(cast(CoverEndDate as string), 'yyyyMMdd') between to_date(cast(effetto as string), 'yyyyMMdd') and to_date(cast(termine as string), 'yyyyMMdd')
    )
      where
        datasource = 'BEY' and contractid='BEY/DE0619954/20151022'
  

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON a inner join sl1_20201231_v20220529_03.ANAGCC_CON b on (a.nposizione=b.nposizione) where a.nposizione='20659238' and npolizza='DE0619954'

-- COMMAND ----------

-- MAGIC %md Validate

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON

-- COMMAND ----------

with CTE as (
  select
    case
      When InsuredId like 'BEY@%' then split(InsuredId, '@') [1]
      Else InsuredId
    End as CustomerId,
    Case
      When InsuredId like 'BEY@%' then ragionesociale
      Else d_ornol_short_name
    End as CustomerName,
    a.*
  from
    db_202012_20220610_000047_502.contracts a
    left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (
      InsuredId = Id
      and b.effect_to_dat > current_date()
    )
    left join sl1_20201231_v20220529_03.ALLCURPOCA_CON c on (a.PolicyId = c.npolizza)
    inner join sl1_20201231_v20220529_03.ANAGCC_CON d on (
      c.nposizione = d.nposizione
      and to_date(cast(CoverEndDate as string), 'yyyyMMdd') between to_date(cast(effetto as string), 'yyyyMMdd')
      and to_date(cast(termine as string), 'yyyyMMdd')
    )
  where
    datasource = 'BEY'
)
select
  contractid,
  count(*)
from
  CTE
group by
  contractid
having
  count(*) > 1
order by
  2 desc --and to_date(cast(RiskPEriodStartDate as string), 'yyyyMMdd') between to_date(cast(effetto as string), 'yyyyMMdd') and to_date(cast(termine as string), 'yyyyMMdd')

-- COMMAND ----------

 select
    case
      When InsuredId like 'BEY@%' then split(InsuredId, '@') [1]
      Else InsuredId
    End as CustomerId,
    Case
      When InsuredId like 'BEY@%' then ragionesociale
      Else d_ornol_short_name
    End as CustomerName,
    a.*,
    b.*,
    c.*,
    d.*
  from
    db_202012_20220610_000047_502.contracts a
    left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (
      InsuredId = Id
      and b.effect_to_dat > current_date()
    )
    left join sl1_20201231_v20220529_03.ALLCURPOCA_CON c on (a.PolicyId = c.npolizza)
    inner join sl1_20201231_v20220529_03.ANAGCC_CON d on (
      c.nposizione = d.nposizione
      and to_date(cast(CoverEndDate as string), 'yyyyMMdd') between to_date(cast(effetto as string), 'yyyyMMdd')
      and to_date(cast(termine as string), 'yyyyMMdd')
    )
  where
    datasource = 'BEY' and contractid='BEY/DE0619954/20151022'



-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ANAGCC_CON

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ANAGCC_CON where nposizione='BA060175DON'

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON a inner join sl1_20201231_v20220529_03.ANAGCC_CON b on (a.nposizione=b.nposizione) where a.nposizione='TO815596XXX'

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.ALLCURPOCA_CON 

-- COMMAND ----------

-- MAGIC %md ANAGCC_CON Incude the CustomerId and Name however we have to join it with ALLCURPOCA_CON via nposizione

-- COMMAND ----------

select a.nposizione as CustomerID, ragionesociale as CustomerName, SymphonyId from sl1_20201231_v20220529_03.ALLCURPOCA_CON a inner join sl1_20201231_v20220529_03.ANAGCC_CON b on (a.nposizione=b.nposizione) where npolizza='AN0018307'

-- COMMAND ----------

-- MAGIC %md In the case below we don't have SymphonyId and InsuredId is BEY@VE170525INT then we have to exctract the CustomerId and CustomerName from the source belows

-- COMMAND ----------

select a.nposizione as CustomerID, ragionesociale as CustomerName, SymphonyId from sl1_20201231_v20220529_03.ALLCURPOCA_CON a inner join sl1_20201231_v20220529_03.ANAGCC_CON b on (a.nposizione=b.nposizione) where npolizza=371000589

-- COMMAND ----------

select
  a.id as PolicyID,
  a.ORCUR_ORNNN_ID as CustomerID,
  d_ornol_short_name as CustomerName
from
  sl1_20201231_v20220529_03.TBBU_POLICIES a,
  sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b
where
  a.ORCUR_ORNNN_ID = b.id
  and b.effect_to_dat > current_date()

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.TBBU_POLICIES

-- COMMAND ----------

select ID as CustomerID, d_ornol_short_name as CustomerName from sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS where id=11678965

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.TBOR_CUSTOMER_DETAILS

-- COMMAND ----------

-- MAGIC %md #NAV

-- COMMAND ----------

-- MAGIC %md Done

-- COMMAND ----------

select 
case 
When InsuredId like 'NAV@%' then split(InsuredId,'@')[1] 
Else InsuredId
End as CustomerId,
Case
When InsuredId like 'NAV@%' then name
Else d_ornol_short_name
End as CustomerName,
a.*
from db_202012_20220610_000047_502.contracts a left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (InsuredId=Id and b.effect_to_dat > current_date()) left join sl1_20201231_v20220529_03.Contact c on (split(InsuredId,'@')[1])=no_
where datasource='NAV'

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.Contact 

-- COMMAND ----------

select * from db_202012_20220610_000047_502.contracts a where datasource='NAV'

-- COMMAND ----------

-- MAGIC %md #NST

-- COMMAND ----------

-- MAGIC %md Done

-- COMMAND ----------

select InsuredId as CustomerID, d_ornol_short_name as CustomerName ,a.* from db_202012_20220610_000047_502.contracts  a left join sl1_20201231_v20220529_03.TBOR_NON_NCM_ORGANISATIONS b on (InsuredId=Id and b.effect_to_dat > current_date())  where datasource='NST'

-- COMMAND ----------

--tbor_organisation_details

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC dbutils.fs.ls("/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/CI/Symphony/TBOR_ORGANISATION_DETAILS")

-- COMMAND ----------

create table sl1_20201231_v20220529_03.TBOR_ORGANISATION_DETAILS
using orc
options (path="/mnt/sl1/DATA/SL1/20201231.v20220529_03/SourceData/CI/Symphony/TBOR_ORGANISATION_DETAILS/*")

-- COMMAND ----------

select * from db_202012_20220610_000047_502.contracts where insuredid like 'BEY@%'

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.TBOR_ORGANISATION_DETAILS --where ornnn_id like 'NAV@%'

-- COMMAND ----------

select * from sl1_20201231_v20220529_03.TBOR_ORGANISATION_DETAILS order by ornnn_id asc
