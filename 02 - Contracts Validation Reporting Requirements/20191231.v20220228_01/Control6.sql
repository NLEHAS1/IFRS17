-- Databricks notebook source
select distinct mainproduct from MI2022.Contracts_SL1_20191231_v20220228_01 where DataSource='SYM'

-- COMMAND ----------

create view MI2022.Control6 as
select * from MI2022.Contracts_SL1_20191231_v20220228_01 where DataSource='SYM' and mainproduct!='CI-ST'

-- COMMAND ----------

select count(*) from MI2022.Contracts_SL1_20191231_v20220228_01 where DataSource='SYM' and mainproduct!='CI-ST'

-- COMMAND ----------

select * from MI2022.Control6

-- COMMAND ----------

select distinct unit from mi2022.control6

-- COMMAND ----------

select distinct producttype from mi2022.control6
