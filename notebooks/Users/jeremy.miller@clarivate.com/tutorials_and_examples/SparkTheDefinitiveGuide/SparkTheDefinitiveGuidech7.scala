// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 6: Aggregations
// MAGIC * In select statements
// MAGIC * Group by - choosing a key and an aggregation function
// MAGIC * window - choosing a key, an aggregation function, and defined row relationship
// MAGIC * grouping set - aggregate at multiple different levels
// MAGIC  * rollup - keys and functions, summarized hierarchically
// MAGIC  * cube - keys and functions, summarized across all combinations of columns

// COMMAND ----------

