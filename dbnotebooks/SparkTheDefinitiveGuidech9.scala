// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 10: Spark SQL
// MAGIC * Can run SQL queries against views or tables organized into databases
// MAGIC * Use system functions or user-defined functions
// MAGIC * Analyze query plans
// MAGIC * SQL and Scala transformations compile to the same underlying code

// COMMAND ----------

val person = Seq(
  (0, "Bill Chambers", 0, Seq(100)),
  (1, "Matei Zaharia", 1, Seq(500,250,100)),
  (2, "Michael Ambrust", 1, Seq(250,100)))
  .toDF("id", "name", "graduate_program", "spark_status")

val graduateProgram = Seq(
  (0, "Masters", "School of Information", "UC Berkeley"),
  (2, "Masters", "EECS", "UC Berkeley"),
  (1, "PhD", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")

val sparkStatus = Seq(
  (500, "Vice President"),
  (250, "PMC Member"),
  (100, "Contributor"))
  .toDF("id", "status")

// COMMAND ----------

person.createOrReplaceTempView("person")

// COMMAND ----------

spark.sql(""" 
select id, name
from person
""").show

// COMMAND ----------

spark.read.json("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/flight-data/json/2015-summary.json").createOrReplaceTempView("flightData")

// COMMAND ----------

spark.sql(""" 
select DEST_COUNTRY_NAME, sum(COUNT)
from flightData
group by DEST_COUNTRY_NAME
""").where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10").show

// COMMAND ----------

val flightData = spark.read.json("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/flight-data/json/2015-summary.json")

// COMMAND ----------

