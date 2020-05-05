// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 4: Structured API Overview

// COMMAND ----------

// MAGIC %md All operations are translated and performed on *Spark* types, not Scala or Python types.

// COMMAND ----------

val df = spark.range(500).toDF("number")
df.select(df.col("number") + 10)
res0.show

// COMMAND ----------

// MAGIC %python
// MAGIC df = spark.range(500).toDF("number")
// MAGIC df.select(df['number'] + 10)
// MAGIC df.show()