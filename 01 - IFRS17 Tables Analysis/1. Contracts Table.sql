-- Databricks notebook source
-- MAGIC %md #Database Analysis

-- COMMAND ----------

-- MAGIC %md The versions of the database delivered to SAS as data input for IFRS17 Calculation Engine

-- COMMAND ----------

show databases

-- COMMAND ----------

-- MAGIC %md Using the last version of the database

-- COMMAND ----------

use db_2021_10_13_192247_fullpatched_4fab15d58801e634c96f83ff5e05d64d936e1fcd;
show tables

-- COMMAND ----------

select distinct datasource from contracts

-- COMMAND ----------

select * from  MI2022.Contracts_202012_20220413_140845_207

-- COMMAND ----------

-- MAGIC %md #Contracts Table Profiling

-- COMMAND ----------

-- MAGIC %md General information for Contracts table such as data type, row numbers, size ..etc

-- COMMAND ----------

ANALYZE TABLE  MI2022.Contracts_201912_20220413_133443_206 COMPUTE STATISTICS ;
DESC EXTENDED  MI2022.Contracts_201912_20220413_133443_206;

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC library(SparkR)
-- MAGIC ContractsR <- sql("Select * from Contracts")
-- MAGIC str(ContractsR)

-- COMMAND ----------

-- MAGIC %md 100 rows exctract from the Contracts table

-- COMMAND ----------

select * from contracts ;

-- COMMAND ----------

-- MAGIC %md Summary statistics for Contracts table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC ContractsPy = sqlContext.sql('Select * from  MI2022.contracts_202012_20220528_010617_253')

-- COMMAND ----------

-- MAGIC %py
-- MAGIC Contractsdf=ContractsPy.summary()
-- MAGIC display(Contractsdf)

-- COMMAND ----------

-- MAGIC %md Number of null values in the Contracts table

-- COMMAND ----------

-- MAGIC %py from pyspark.sql.functions import isnan, when, count, col
-- MAGIC Contractsnull=ContractsPy.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in ContractsPy.columns])
-- MAGIC display(Contractsnull)

-- COMMAND ----------

select * from MI2022.contracts_202012_20220528_010617_253 where mainproduct is null

-- COMMAND ----------

-- MAGIC %md Number of unique values for all columns in Contracts table

-- COMMAND ----------

SELECT 'Number of Unique Values' AS RowValue,
COUNT(DISTINCT ValuationDate) AS ValuationDate,
COUNT(DISTINCT ContractId) AS ContractId,
COUNT(DISTINCT DataSource) AS DataSource,
COUNT(DISTINCT PolicyId) AS PolicyId,
COUNT(DISTINCT ManagedTogetherId) AS ManagedTogetherId,
COUNT(DISTINCT InsurerId) AS InsurerId,
COUNT(DISTINCT InsuredId) AS InsuredId,
COUNT(DISTINCT BeneficiaryId) AS BeneficiaryId,
COUNT(DISTINCT CustomerCountry) AS CustomerCountry,
COUNT(DISTINCT CoverStartDate) AS CoverStartDate,
COUNT(DISTINCT CoverEndDate) AS CoverEndDate,
COUNT(DISTINCT BoundDate) AS BoundDate,
COUNT(DISTINCT ContractInceptionDate) AS ContractInceptionDate,
COUNT(DISTINCT ContractIssueDate) AS ContractIssueDate,
COUNT(DISTINCT RiskPeriodStartDate) AS RiskPeriodStartDate,
COUNT(DISTINCT RiskPeriodEndDate) AS RiskPeriodEndDate,
COUNT(DISTINCT Cancellability) AS Cancellability,
COUNT(DISTINCT InitialProfitabilityClassing) AS InitialProfitabilityClassing,
COUNT(DISTINCT ProductType) AS ProductType,
COUNT(DISTINCT MainProduct) AS MainProduct,
COUNT(DISTINCT Unit) AS Unit,
COUNT(DISTINCT MainUnit) AS MainUnit
--COUNT(DISTINCT OptionId) AS OptionId/ Not available in db_2021_09_07_140300_f5f6795fc7410aad2a0a09b6ccdc091029237cf3
FROM Contracts

-- COMMAND ----------

-- MAGIC %md #Primary Keys Analysis 

-- COMMAND ----------

-- MAGIC %md ##ContractId

-- COMMAND ----------

-- MAGIC %md The code below shows if there's any ContractID with zero value

-- COMMAND ----------

select count(*) from MI2022.contracts_202012_20220528_010617_253 WHERE CONTRACTID=0

-- COMMAND ----------

-- MAGIC %md The code below shows if the contract ids are recurred in the dataset or not

-- COMMAND ----------

SELECT contractid, COUNT(contractid)
FROM MI2022.contracts_202012_20220528_010617_253
GROUP BY contractid
HAVING COUNT(contractid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md The number of unique contract ids

-- COMMAND ----------

select count(distinct contractid) from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md The number of contract ids rows.

-- COMMAND ----------

select count(*) from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md One ContractId can have only one ManagedTogetherId

-- COMMAND ----------

SELECT contractid, COUNT( distinct ManagedTogetherId)
FROM MI2022.contracts_202012_20220528_010617_253
GROUP BY contractid
HAVING COUNT( distinct ManagedTogetherId)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One ContractId can have only one PolicyID

-- COMMAND ----------

SELECT contractid, COUNT( distinct policyid)
FROM MI2022.contracts_202012_20220528_010617_253
GROUP BY contractid
HAVING COUNT( distinct policyid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md ####Referentioal Integrity

-- COMMAND ----------

-- MAGIC %md Validate the ContractIds in Contracts and Cashflows

-- COMMAND ----------

select contractid from MI2022.contracts_202012_20220528_010617_253
except 
select contractid from MI2022.cashflows_202012_20220528_010617_253

-- COMMAND ----------

select contractid from MI2022.cashflows_202012_20220528_010617_253
except 
select contractid from MI2022.contracts_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md As we can see from the table above that there are contracts in the cashflows table that don't have contact id in the contracts table

-- COMMAND ----------

-- MAGIC %md ##ValuationDate

-- COMMAND ----------

-- MAGIC %md The code below show us the distinct values of valuation date

-- COMMAND ----------

select distinct ValuationDate from MI2022.contracts_202012_20220528_010617_253

-- COMMAND ----------

-- MAGIC %md #ContractIssueDate (Not Primary Key)

-- COMMAND ----------

-- MAGIC %md The code below below shows that some ContractIds have multiple ContractIssueDate

-- COMMAND ----------

SELECT contractid, COUNT( distinct contractissuedate)
FROM MI2022.contracts_202012_20220528_010617_253
GROUP BY contractid
HAVING COUNT( distinct contractissuedate)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md ###Date Sources (not Primary Key)

-- COMMAND ----------

-- MAGIC %md Show the distinct values in DataSource column

-- COMMAND ----------

select distinct datasource from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

-- MAGIC %md ###Unit (not Primary key)

-- COMMAND ----------

-- MAGIC %md Show the distinct values in Unit column

-- COMMAND ----------

select distinct unit from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

select * from MI2022.contracts_201912_20220522_180045_173 where unit=0

-- COMMAND ----------

-- MAGIC %md ###MainUnit (not Primary key)

-- COMMAND ----------

select distinct mainunit from MI2022.contracts_201912_20220528_010617_249

-- COMMAND ----------

select * from contracts where mainunit not in (select distinct mainunit from groupings2)

-- COMMAND ----------

-- MAGIC %md Number of Transactions in each mainunit

-- COMMAND ----------

select mainunit, count(*) from Contracts a left join cashflows b on a.contractid=b.contractid group by mainunit

-- COMMAND ----------

select * from MI2022.contracts_201912_20220522_180045_173 where mainunit=0

-- COMMAND ----------

-- MAGIC %md ###MainProduct (not Primary key)

-- COMMAND ----------

select * from MI2022.contracts_201912_20220528_010617_249 where mainproduct=0

-- COMMAND ----------

select distinct mainproduct,datasource from MI2022.contracts_202006_20220522_180045_175

-- COMMAND ----------

select * from contracts where mainproduct not in (select distinct mainproduct from groupings2)

-- COMMAND ----------

-- MAGIC %md ###PolicyId

-- COMMAND ----------

-- MAGIC %md One PolicyID can have multiple ManagedTogetherId

-- COMMAND ----------

SELECT policyid, COUNT( distinct ManagedTogetherId)
FROM MI2022.Contracts_202012_20220413_140845_207
GROUP BY policyid
HAVING COUNT( distinct ManagedTogetherId)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One policyid can have multiple contractid

-- COMMAND ----------

SELECT policyid, COUNT( distinct contractid)
FROM contracts
GROUP BY policyid
HAVING COUNT( distinct contractid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One PolicyID can have only one InsuredId

-- COMMAND ----------

SELECT policyid, COUNT( distinct InsuredId)
FROM MI2022.contracts_201912_20220528_010617_249
GROUP BY policyid
HAVING COUNT( distinct InsuredId)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One PolicyID can have multiple InsurerId

-- COMMAND ----------

SELECT policyid, COUNT( distinct InsurerId)
FROM MI2022.Contracts_202012_20220413_140845_207
GROUP BY policyid
HAVING COUNT( distinct InsurerId)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md ###ManagedTogetherId

-- COMMAND ----------

-- MAGIC %md One ManagedTogetherId can have multiple ContractIds

-- COMMAND ----------

SELECT ManagedTogetherId, COUNT( distinct contractID)
FROM contracts
GROUP BY ManagedTogetherId
HAVING COUNT( distinct contractID)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md One ManagedTogetherId can have multiple PolicyIds

-- COMMAND ----------

SELECT ManagedTogetherId, COUNT( distinct policyid)
FROM contracts
GROUP BY ManagedTogetherId
HAVING COUNT( distinct policyid)>1
order by 2 desc

-- COMMAND ----------

-- MAGIC %md #Analysis of InsurerId and InsuredId

-- COMMAND ----------

-- MAGIC %md ###InsurerId

-- COMMAND ----------

-- MAGIC %md As we can see from the table below, there are 34 entities fom the entities table found in Cashflows table. 34 entities with true IFRS17CalculationEntity which consist of L3 (203 and 285) and L4 (rest). 

-- COMMAND ----------

select distinct insurerid,a.EntityId,name,ParentEntityId,IFRS17CalculationEntity from entities a left join hierarchies b on (a.EntityId=b.EntityId) where insurerid in (select distinct insurerid from Contracts)

-- COMMAND ----------

-- MAGIC %md ###InsuredId

-- COMMAND ----------

-- MAGIC %md As we can see from the table below, there are 35 entities fom the entities table found in Cashflows table. 34 entities with true IFRS17CalculationEntity which consist of L3 (203 and 285) and L4 (rest). 1 entity (413) with false IFRS17CalculationEntity.

-- COMMAND ----------

select distinct insurerid,a.EntityId,name,ParentEntityId,IFRS17CalculationEntity from entities a left join hierarchies b on (a.EntityId=b.EntityId) where insurerid  in (select distinct insuredid from Contracts)

-- COMMAND ----------

select policyid,count(distinct contractissuedate) from contracts where datasource='SYM' group by policyid having count(distinct contractissuedate)>1

-- COMMAND ----------

select * from MI2022.TBPO_POL_VERSIONS_SL1_20191231_v20220228_01 where bupiy_id=67196

-- COMMAND ----------

select distinct policyid from MI2022.contracts_201912_20220528_010617_249 where datasource='SYM' and ContractissueDate<> contractinceptiondate
