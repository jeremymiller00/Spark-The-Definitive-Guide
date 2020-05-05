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
// MAGIC * Each grouping returns a *RelationalGroupedSet* on which we specify aggregations
// MAGIC * Approximations can save a *lot* of compute

// COMMAND ----------

val dataPath = "s3://databricks-cc/jeremy/spark-the-definitive-guide/data/retail-data/all/online-retail-dataset.csv"
val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(dataPath).coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
df.count()

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md ## All aggregations are available as Functions

// COMMAND ----------

df.select(countDistinct("StockCode")).first().getLong(0)

// COMMAND ----------

df.select(approx_count_distinct("StockCode", 0.1)).first().getLong(0)

// COMMAND ----------

df.select(approx_count_distinct("StockCode", 0.01)).first().getLong(0)

// COMMAND ----------

df.select(min("Quantity"), max("Quantity")).show

// COMMAND ----------

df.select(sum("Quantity").as("Total_Quantity"), avg("Quantity").as("Avg_Quantity")).show

// COMMAND ----------

df.select(avg("Quantity").as("Avg_Quantity"), expr("mean(Quantity)").as("Mean_Quantity")).show

// COMMAND ----------

// MAGIC %md ## Variance and std deviation are performed with sample formula by default 
// MAGIC * variance, stddev
// MAGIC * var_samp, stddev_samp
// MAGIC * var_pop, stddev_pop

// COMMAND ----------

df.select(var_pop("Quantity"), var_samp("Quantity"), stddev_samp("Quantity"), stddev_pop("Quantity")).show

// COMMAND ----------

df.select(skewness("Quantity"), kurtosis("Quantity")).show

// COMMAND ----------

// MAGIC %md ## Correlation and Covariance
// MAGIC * corr, covar_pop, covar_sample

// COMMAND ----------

df.select(corr("Quantity", "UnitPrice"), covar_samp("Quantity", "UnitPrice"), covar_pop("Quantity", "UnitPrice")).show

// COMMAND ----------

// MAGIC %md ## Aggregating to Complex Types

// COMMAND ----------

df.agg(collect_set("Country"), collect_list("Country")).show

// COMMAND ----------

df.groupBy("InvoiceNo", "CustomerId").count.show(5)

// COMMAND ----------

df.groupBy("CustomerId").count.orderBy(desc("count")).show(5)

// COMMAND ----------

df.groupBy("InvoiceNo").agg(expr("count(Quantity)"), expr("sum(Quantity)")).show(5)

// COMMAND ----------

df.groupBy("InvoiceNo").agg("Quantity" -> "avg", "Quantity" -> "stddev_pop").show(5)

// COMMAND ----------

// MAGIC %md ## Window Functions: define aggregation over a specific 'window' of data
// MAGIC * In groupBy each row can only go into one group
// MAGIC * In window each grouping is based on a frame of rows, like a rolling average
// MAGIC  * ranking
// MAGIC  * analytic
// MAGIC  * aggregate

// COMMAND ----------

val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

// COMMAND ----------

display(dfWithDate)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quanity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// COMMAND ----------

val purchaseDenseRank.show

// COMMAND ----------

