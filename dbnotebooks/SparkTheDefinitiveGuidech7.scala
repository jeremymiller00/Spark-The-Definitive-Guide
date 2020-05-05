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

// MAGIC %md ## Variance and std deviation are performed with sample (not population) formula by default 
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

df.select(corr("CustomerID", "UnitPrice"), corr("Quantity", "UnitPrice")).show

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
// MAGIC * Spark suports 3 types:
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
val windowSpec = Window.partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow) // specifies which rows will be included in window relative to current row

// COMMAND ----------

val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

// COMMAND ----------

import org.apache.spark.sql.functions.{dense_rank, rank}
val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)

// COMMAND ----------

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
  .select(
    col("CustomerId")
    ,col("date")
    ,col("Quantity")
//     ,purchaseRank.alias("quantityRank")
    ,purchaseDenseRank.alias("quantityDenseRank")
    ,maxPurchaseQuantity.alias("maxPurchaseQuantity")
  ).show

// COMMAND ----------

// MAGIC %md ## Grouping Sets
// MAGIC A low-level tool for combining sets of aggregations together. Ability to create arbitrary aggregations in their group by statments. *Only available in SQL.*
// MAGIC * Example: get the total quantity of all stock codes and customers
// MAGIC * In other words, what is the total quantity of each item every ordered by each customer

// COMMAND ----------

display(dfWithDate)

// COMMAND ----------

dfWithDate.count

// COMMAND ----------

val dfNotNull = dfWithDate.drop()
dfNotNull.createOrReplaceTempView("dfNotNull")
dfNotNull.count

// COMMAND ----------

// MAGIC %sql
// MAGIC select CustomerId, stockCode, sum(Quantity)
// MAGIC from dfNotNull
// MAGIC group by CustomerId, stockCode
// MAGIC order by CustomerId desc, stockCode desc

// COMMAND ----------

// MAGIC %md ### Can do the same thing with a *grouping set*

// COMMAND ----------

// MAGIC %sql
// MAGIC select CustomerId, stockCode, sum(Quantity)
// MAGIC from dfNotNull
// MAGIC group by CustomerId, stockCode grouping sets((CustomerId, stockCode))
// MAGIC order by CustomerId desc, stockCode desc

// COMMAND ----------

// MAGIC %md ## Now, what if I also want to include the total number of items, regardless of customer or stock code?

// COMMAND ----------

// MAGIC %sql
// MAGIC select CustomerId, stockCode, sum(Quantity)
// MAGIC from dfNotNull
// MAGIC group by CustomerId, stockCode grouping sets((CustomerId, stockCode),())
// MAGIC order by CustomerId desc, stockCode desc

// COMMAND ----------

// MAGIC %md ## Rollups and Cubes are the DataFrame versions of a Grouping Set
// MAGIC * Rollup is a multi-dimensional aggregation that performs a variety of group-by style calculations

// COMMAND ----------

val rolledUpDF = dfNotNull.rollup("Date", "Country").agg(sum("Quantity"))
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
  .orderBy("Date", "Country")

rolledUpDF.show // where you see null values are the grand totals

// COMMAND ----------

dfNotNull.groupBy("Date", "Country").agg(sum("Quantity")).orderBy("Date", "Country").show

// COMMAND ----------

// MAGIC %md ## Cube takes a rollup deeper. Across dimensions, rather than hierarchically.
// MAGIC * Can we make a table that includes:
// MAGIC  * Total across all dates and countries
// MAGIC  * Total for each date across all countries
// MAGIC  * Total for each country on each date
// MAGIC  * Total for each country across all dates

// COMMAND ----------

display(dfNotNull.cube("Date", "Country").agg(sum(col("Quantity")))
  .select("Date", "Country", "sum(Quantity)").orderBy("Date")) // where date is null, it is a total for that country

// COMMAND ----------

// MAGIC %md ## With hierarchical groupings, a grouping ID identifies the level of grouping for that row.

// COMMAND ----------

display(dfNotNull.cube("customerId", "stockCode").agg(grouping_id(), sum("Quantity"))
//   .orderBy(expr("grouping_id()").desc()
  .filter($"grouping_id()" === 0))

// COMMAND ----------

// MAGIC %md ## Pivot

// COMMAND ----------

val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
display(pivoted)

// COMMAND ----------

