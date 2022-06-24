-- Databricks notebook source
show databases

-- COMMAND ----------

use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;
Show tables

-- COMMAND ----------

-- MAGIC %md #Entities Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for Entities table such as data type, row numbers, size ..etc

-- COMMAND ----------

describe formatted entities

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC library(SparkR)
-- MAGIC EntitiesR <- sql("Select * from entities")
-- MAGIC str(EntitiesR)

-- COMMAND ----------

-- MAGIC %md Rows exctract from the Contracts table

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from entities where insurerid='P#675' or insurerid='External'

-- COMMAND ----------

select * from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid) where topartyid='External' or frompartyid='External'

-- COMMAND ----------

-- MAGIC %md Summary statistics for Entities table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC EntitiesPy = sqlContext.sql('Select * from MI2022.entities_202006_20220522_180045_175')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC EntitiesDf=EntitiesPy.summary()
-- MAGIC display(EntitiesDf)

-- COMMAND ----------

-- MAGIC %md Number of null values in the Entities table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC EntitiesNull=EntitiesPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in EntitiesPy.columns])
-- MAGIC display(EntitiesNull)

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in Entities table

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT EntityId) AS EntityId,
COUNT(DISTINCT Name) AS Name,
COUNT(DISTINCT LegalName) AS LegalName,
COUNT(DISTINCT EntityType) AS EntityType,
COUNT(DISTINCT Country) AS Country,
COUNT(DISTINCT InsuranceActivity) AS InsuranceActivity,
COUNT(DISTINCT ReportingCurrency) AS ReportingCurrency,
COUNT(DISTINCT FunctionalCurrency) AS FunctionalCurrency,
COUNT(DISTINCT ReportingLanguage) AS ReportingLanguage,
COUNT(DISTINCT Role) AS Role,
COUNT(DISTINCT Purpose) AS Purpose
FROM Entities

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##ValuationDate

-- COMMAND ----------

-- MAGIC %md The code below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from MI2022.entities_202006_20220522_180045_175

-- COMMAND ----------

-- MAGIC %md ##EntityId

-- COMMAND ----------

-- MAGIC %md The code belows shows that the EntityId in unique

-- COMMAND ----------

select EntityId, count(EntityId) from MI2022.entities_201912_20220522_180045_173
group by EntityId
having count(EntityId)>1
order by 2

-- COMMAND ----------

-- MAGIC %md #Join with Contracts

-- COMMAND ----------

-- MAGIC %md The code below shows us that Insurerid and Insuredid from Contracts table don't belong to entityid from Entities table

-- COMMAND ----------

select insurerid from contracts where insurerid in (select entityid from entities)

-- COMMAND ----------

select insuredid from contracts where insuredid in (select entityid from entities)

-- COMMAND ----------

-- MAGIC %md The code below shows us that Insurerid and Insuredid from Contracts table belong to Insurerid from Entities table.

-- COMMAND ----------

select distinct insurerid from contracts where insurerid in (select insurerid from entities)

-- COMMAND ----------

select distinct insuredid from contracts where insuredid in (select insurerid from entities)

-- COMMAND ----------

-- MAGIC %md #Joining Cashflows & Contracts with Entities

-- COMMAND ----------

-- MAGIC %md The join isn't validated

-- COMMAND ----------

select distinct FromPartyId from cashflows where FromPartyId in (select insurerid from entities)


-- COMMAND ----------

select distinct ToPartyId from cashflows where ToPartyId in (select insurerid from entities)

-- COMMAND ----------

-- MAGIC %md #Direct insurance contracts and reinsurance held contracts

-- COMMAND ----------

-- MAGIC %md ###DC

-- COMMAND ----------

Create view DC as 
select 
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
InsurerId,
InsuredId,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
b.DataSource as CashflowsDataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid)
where insurerid in (select insurerid from entities) and insuredid not in (select insurerid from entities)

-- COMMAND ----------

select * from DC

-- COMMAND ----------

select distinct FlowSequenceId from DC

-- COMMAND ----------

-- MAGIC %md ###RHC

-- COMMAND ----------

Create view RHC as 
select 
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
InsurerId,
InsuredId,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
b.DataSource as CashflowsDataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid)
where insuredid in (select insurerid from entities) and insurerid not in (select insurerid from entities)

-- COMMAND ----------

select * from RHC

-- COMMAND ----------

select * from entities where insurerid='17681259'

-- COMMAND ----------

select distinct FlowSequenceId from RHC

-- COMMAND ----------

-- MAGIC %md #Incoming and outgoing cash flows

-- COMMAND ----------

-- MAGIC %md ###CFIncoming

-- COMMAND ----------

Create view CFIncoming as 
select 
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
InsurerId,
InsuredId,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
b.DataSource as CashflowsDataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid)
where ToPartyId in (select insurerid from entities) and FromPartyId not in (select insurerid from entities)

-- COMMAND ----------

select * from CFIncoming

-- COMMAND ----------

select Count(*) from CFIncoming

-- COMMAND ----------

-- MAGIC %md ###CFOutgoing

-- COMMAND ----------

Create view CFOutgoing as 
select 
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
InsurerId,
InsuredId,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
ToPartyId,
RiskCounterPartyId,
CountryOfRisk,
b.DataSource as CashflowsDataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from contracts a left join cashflows b on (a.valuationdate=b.valuationdate and a.contractid=b.contractid)
where FromPartyId in (select insurerid from entities) and ToPartyId not in (select insurerid from entities)

-- COMMAND ----------

select * from CFOutgoing

-- COMMAND ----------

select Count(*) from CFOutgoing

-- COMMAND ----------

-- MAGIC %md #PK Validation

-- COMMAND ----------

select count(*) from entities

-- COMMAND ----------

select count(distinct valuationdate, entityid) from entities

-- COMMAND ----------

-- MAGIC %md #EntityHierarchies

-- COMMAND ----------

drop view EntityHierarchies

-- COMMAND ----------

Create View EntityHierarchies as
select
  a.ValuationDate,
  a.InsurerId,
  b.EntityId as L1EntityID,
  a.Name as L1EnitiyName,
  a.EntityType as L1EntityType,
  a.Country as L1Country,
  b.IFRS17CalculationEntity as L1IFRS17CalculationEntity,
  b.IFRS17ReportingEntity as L1IFRS17ReportingEntity,
  b.IFRS17ConsolidationEntity as L1IFRS17ConsolidationEntity,
  
  b.ParentEntityId as L2EntityId,
  c.Name as L2EntityName,
  c.EntityType as L2EntityType,
  c.country as L2Country,
  d.IFRS17CalculationEntity as L2IFRS17CalculationEntity,
  d.IFRS17ReportingEntity as L2IFRS17ReportingEntity,
  d.IFRS17ConsolidationEntity as L2IFRS17ConsolidationEntity,
  
  d.ParentEntityId as L3EntityId,
  e.Name as L3EntityName,
  e.EntityType as L3EntityType,
  e.country as L3Country,
  f.IFRS17CalculationEntity as L3IFRS17CalculationEntity,
  f.IFRS17ReportingEntity as L3IFRS17ReportingEntity,
  f.IFRS17ConsolidationEntity as L3IFRS17ConsolidationEntity,
  
  f.ParentEntityId as L4EntityId,
  g.Name as L4EntityName,
  g.EntityType as L4EntityType,
  g.country as L4Country,
  h.IFRS17CalculationEntity as L4IFRS17CalculationEntity,
  h.IFRS17ReportingEntity as L4IFRS17ReportingEntity,
  h.IFRS17ConsolidationEntity as L4IFRS17ConsolidationEntity
from
  entities a inner join hierarchies b on (a.ValuationDate = b.ValuationDate and a.EntityId = b.EntityId) 
  inner join entities c on (b.ValuationDate=c.ValuationDate and b.ParentEntityId=c.EntityId )
  inner join hierarchies d on (c.ValuationDate = d.ValuationDate and c.EntityId = d.EntityId) 
  inner join entities e on (d.ValuationDate=e.ValuationDate and d.ParentEntityId=e.EntityId )
  inner join hierarchies f on (d.ValuationDate = f.ValuationDate and d.EntityId = f.EntityId) 
  inner join entities g on (f.ValuationDate=g.ValuationDate and f.ParentEntityId=g.EntityId )
  inner join hierarchies h on (g.ValuationDate = h.ValuationDate and g.EntityId = h.EntityId)
   

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from Hierarchies

-- COMMAND ----------

select * from EntityHierarchies

-- COMMAND ----------

Create View EntityHierarchies as
select
  a.ValuationDate,
  a.InsurerId,
  b.EntityId as L1EntityID,
  b.Name as L1EnitiyName,
  b.EntityType as L1EntityType,
  b.Country as L1Country,
  a.IFRS17CalculationEntity as L1IFRS17CalculationEntity,
  a.IFRS17ReportingEntity as L1IFRS17ReportingEntity,
  a.IFRS17ConsolidationEntity as L1IFRS17ConsolidationEntity,
  c.EntityId as L2EntityId,
  c.Name as L2EntityName,
  c.EntityType as L2EntityType,
  c.country as L2Country,
  d.IFRS17CalculationEntity as L2IFRS17CalculationEntity,
  d.IFRS17ReportingEntity as L2IFRS17ReportingEntity,
  d.IFRS17ConsolidationEntity as L2IFRS17ConsolidationEntity
from
  hierarchies a
  left join entities b on (a.ValuationDate = b.ValuationDate and a.EntityId = b.EntityId)
  left join entities c on (a.ValuationDate = c.ValuationDate and a.ParentEntityId = c.EntityId)
  left join hierarchies d on (c.ValuationDate = d.ValuationDate and d.EntityId = c.EntityId)

-- COMMAND ----------

select * from EntityHierarchies

-- COMMAND ----------

desc entities

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from  Hierarchies

-- COMMAND ----------

drop view EntityHierarchies2

-- COMMAND ----------

Create View EntityHierarchies2 as
select a.ValuationDate, a.EntityId,a.InsurerId,a.Name,a.LegalName,a.EntityType,a.Country,a.InsuranceActivity,a.ReportingCurrency,a.FunctionalCurrency,a.ReportingLanguage,a.Role,a.Purpose,b.HierarchyId,b.ParentEntityId,b.IFRS17CalculationEntity,IFRS17ReportingEntity,IFRS17ConsolidationEntity
from entities a inner join hierarchies b on (a.ValuationDate=b.ValuationDate and a.EntityId=b.EntityId) 

-- COMMAND ----------

select * from EntityHierarchies2

-- COMMAND ----------

select EntityId from entities where EntityId not in (select EntityId from hierarchies)

-- COMMAND ----------

select * from EntityHierarchies2

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from Hierarchies where entityid='461'

-- COMMAND ----------

Create view DC_RHC_CFIncoming_CFOutgoing2 as
select
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
a.InsurerId,
f.Name as InsurerName,
InsuredId,
d.Name as InsuredName,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
FromPartyId,
h.Name as FromPartyName,
ToPartyId,
g.Name as ToPartyName,
RiskCounterPartyId,
CountryOfRisk,
b.DataSource as CashflowsDataSource,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment,
CASE
WHEN  (f.IFRS17CalculationEntity='true'  and d.IFRS17CalculationEntity='false') THEN "DC"
WHEN (d.IFRS17CalculationEntity='true' and f.IFRS17CalculationEntity='false') THEN "RHC"
Else "Leftovers"
End AS ContractType,
CASE
WHEN  (g.IFRS17CalculationEntity='true' and h.IFRS17CalculationEntity='false') THEN "CFIncoming"
WHEN (h.IFRS17CalculationEntity='true' and g.IFRS17CalculationEntity='false') THEN "CFOutgoing"
Else "Leftovers"
End AS CFIncomingOutgoing
from Contracts a left join cashflows b on (a.ValuationDate=b.ValuationDate and a.ContractId=b.ContractID)
left join EntityHierarchies2 f on (a.ValuationDate=f.ValuationDate and a.insurerid=f.insurerid) left join EntityHierarchies2 d on (a.ValuationDate=d.ValuationDate and a.insuredid=d.insurerid ) 
left join EntityHierarchies2 g on (b.ValuationDate=g.ValuationDate and b.ToPartyId=g.insurerid) left join EntityHierarchies2 h on (b.ValuationDate=h.ValuationDate and b.FromPartyId=h.insurerid)

-- COMMAND ----------



-- COMMAND ----------

Select distinct insurerid from EntityHierarchies2 where insurerid not in (select distinct insurerid from contracts )

-- COMMAND ----------

Select distinct insurerid from EntityHierarchies2 where insurerid not in (select distinct insuredid from contracts )

-- COMMAND ----------

Select distinct insurerid from EntityHierarchies2 where insurerid not in (select distinct frompartyid from cashflows )

-- COMMAND ----------

Select distinct insurerid from EntityHierarchies2 where insurerid not in (select distinct topartyid from cashflows )

-- COMMAND ----------

select * from contracts where insuredid='P#615'

-- COMMAND ----------

select * from entities where insurerid='P#615'

-- COMMAND ----------

select * from EntityHierarchies2

-- COMMAND ----------

select * from Hierarchies

-- COMMAND ----------

select * from contracts where contractid='SYM / 623257 / 19890101'

-- COMMAND ----------

select * from cashflows where contractid='SYM / 623257 / 19890101'

-- COMMAND ----------

select * from datedistributions where datedistributionid='-1658581090'

-- COMMAND ----------

select Insurerid, a.EntityID,Name,Country,ParentEntityId,IFRS17CalculationEntity,IFRS17ReportingEntity,IFRS17ConsolidationEntity from entities a inner join hierarchies b on (a.EntityID=b.EntityID)

-- COMMAND ----------

select * from entities

-- COMMAND ----------

select * from hierarchies where EntityId=ParentEntityId

-- COMMAND ----------

select * from contracts

-- COMMAND ----------

drop view part1

-- COMMAND ----------

Create view part1 as
select
  a.ValuationDate,
  a.ContractId,
  a.DataSource,
  PolicyId,
  ManagedTogetherId,
  a.InsurerId,
  b.EntityId as InsurerEId,
  b.Name as InsurerName,
  c.IFRS17CalculationEntity as InsurerIFRS17CalculationEntityFlg,
  a.InsuredId,
  d.EntityID as InsuredEId,
  d.Name as InsuredName,
  e.IFRS17CalculationEntity as InsuredIFRS17CalculationEntityFlg,
  BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
a.MainProduct,
Unit,
a.MainUnit,
GroupingKey,
left(ContractIssueDate,4) as CohortYear,
MAIN_INSURANCE_CONTRACT_GROUP_ID,
z.CalculationEntity,
intra_group_elimination_flg,
direct_contract_flg,
rein_contract_flg
from
  contracts a
  left join Entities b on (a.insurerid = b.insurerid)
  left join Hierarchies c on (b.entityid = c.entityid)
  left join Entities d on (a.InsuredId = d.insurerid)
  left join Hierarchies e on (d.entityid = e.entityid)
  left join Groupings2 g on ( a.mainunit = g.mainunit and a.mainproduct = g.mainproduct)
  left join dcrhc z on (a.contractid=z.contractid and GROUP_TYPE_CD='SUBGROUP')
  

-- COMMAND ----------

select * from part1 where contractid='P/Atradius China Global/GLB-CHN/CIM/CI-ST/16681256/16270147/331/20201201'

-- COMMAND ----------

select * from contracts where contractid='P/Atradius China Global/GLB-CHN/CIM/CI-ST/16681256/16270147/331/20201201'

-- COMMAND ----------

select * from Entities where insurerid=16270147

-- COMMAND ----------

Create view part2 as
select
  a.ValuationDate,
  ContractId,
  a.DataSource,
  a.FromPartyId,
  b.Name as FromPartyName,
  c.IFRS17CalculationEntity as FromPartyIFRS17CalculationEntityFlg,
  a. ToPartyId,
  d.Name as ToPartyName,
  e.IFRS17CalculationEntity as ToPartyIFRS17CalculationEntityFlg,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
RiskCounterPartyId,
CountryOfRisk,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from
  cashflows a
  left join Entities b on (a.FromPartyId = b.insurerid)
  left join Hierarchies c on (b.entityid = c.entityid)
  left join Entities d on (a.ToPartyId = d.insurerid)
  left join Hierarchies e on (d.entityid = e.entityid)

-- COMMAND ----------

desc part2

-- COMMAND ----------

create view Flags as
select 
a.ValuationDate,
a.ContractId,
a.DataSource as ContractsDataSource,
PolicyId,
ManagedTogetherId,
InsurerId,
InsurerName,
InsurerIFRS17CalculationEntityFlg,
InsuredId,
InsuredName,
InsuredIFRS17CalculationEntityFlg,
BeneficiaryId,
CustomerCountry,
CoverStartDate,
CoverEndDate,
BoundDate,
ContractInceptionDate,
ContractIssueDate,
RiskPeriodStartDate,
RiskPeriodEndDate,
Cancellability,
InitialProfitabilityClassing,
ProductType,
MainProduct,
Unit,
MainUnit,
b.DataSource as CashflowsDataSource,
FromPartyId,
FromPartyName,
FromPartyIFRS17CalculationEntityFlg,
ToPartyId,
ToPartyName,
ToPartyIFRS17CalculationEntityFlg,
FutureStateId,
FutureStateProbability,
CashFlowId,
CashFlowType,
CashFlowSubType,
RiskCounterPartyId,
CountryOfRisk,
ModelId,
InvoiceId,
ClaimId,
FlowSequenceId,
DateDistributionId,
RiskCurrency,
TransactionCurrency,
Amount,
CounterpartyDefaultAdjustment
from part1 a left join part2 b on (a.contractid=b.contractid)

-- COMMAND ----------

select * from flags where FromPartyName is not null and ToPartyName is not null

-- COMMAND ----------

select * from flags where contractid='2038@-1@3801684@21758227'

-- COMMAND ----------

select distinct b.entityid from cashflows a left join entities b on (a.frompartyid=b.insurerid)

-- COMMAND ----------

select * from entities
