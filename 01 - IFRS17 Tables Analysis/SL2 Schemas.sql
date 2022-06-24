-- Databricks notebook source
Create schema 201912_20220602_000954_355;
CREATE table 201912_20220602_000954_355.cashflows
USING orc OPTIONS (path "/mnt/sl2/201912_20220602_000954_355/CashFlows/1");
CREATE table 201912_20220602_000954_355.contracts
USING orc OPTIONS (path "/mnt/sl2/201912_20220602_000954_355/Contracts/1");
CREATE table 201912_20220602_000954_355.datedistributions_201912_20220602_000954_355
USING orc OPTIONS (path "/mnt/sl2/201912_20220602_000954_355/DateDistributions/1");
CREATE table 201912_20220602_000954_355.entities
USING orc OPTIONS (path "/mnt/sl2/201912_20220602_000954_355/Entities/1");
CREATE table 201912_20220602_000954_355.fxrates
USING orc OPTIONS (path "/mnt/sl2/201912_20220602_000954_355/FxRates/1")

-- COMMAND ----------

show tables in 201912_20220602_000954_355

-- COMMAND ----------

show databases

-- COMMAND ----------

desc schema db_201912_20220603_000035_370
