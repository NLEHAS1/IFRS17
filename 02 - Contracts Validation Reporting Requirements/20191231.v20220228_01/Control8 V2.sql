-- Databricks notebook source
-- MAGIC %md Based on Amir req

-- COMMAND ----------

select Unit, MainUnit, PolicyID,popgg_id from MI2022.Control5 where datasource='SYM' and RiskPeriodEndDate > 20191231
