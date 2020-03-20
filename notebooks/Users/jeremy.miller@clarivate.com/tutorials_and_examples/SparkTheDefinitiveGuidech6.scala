// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 6: Working with Different Types of Data

// COMMAND ----------

val df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/retail-data/by-day/2010-12-01.csv")

// COMMAND ----------

df.printSchema

// COMMAND ----------

df.createOrReplaceTempView("dfTable")

// COMMAND ----------

df.show

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md ## Convert Native Types to Spark Types

// COMMAND ----------

df.select(lit(5), lit("five"), lit(5.0)).printSchema

// COMMAND ----------

df.where(col("InvoiceNo").equalTo("536365")).select("InvoiceNo", "Description").show(false)

// COMMAND ----------

df.where("InvoiceNo = 536365").select("InvoiceNo", "Description").show(false)

// COMMAND ----------

// MAGIC %md ### Can specify filters as variables

// COMMAND ----------

val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.where(col("StockCode").isin("DOT")).where(priceFilter.or(descripFilter)).show(false)

// COMMAND ----------

// MAGIC %md ### Can specify a Boolean Column

// COMMAND ----------

val DOTCodeFilter = col("StockCode") === "DOT"
val priceFilter = col("UnitPrice") > 600
val descripFilter = col("Description").contains("POSTAGE")
df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
  .where("isExpensive")
  .select("unitPrice", "isExpensive")
  .show

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   UnitPrice
// MAGIC   ,(StockCode = 'DOT' and (UnitPrice > 600 or instr(Description, 'POSTAGE') >= 1)) as isExpensive
// MAGIC from 
// MAGIC   dfTable
// MAGIC where 
// MAGIC   (StockCode = 'DOT' and 
// MAGIC   (UnitPrice > 600 or 
// MAGIC     instr(Description, "POSTAGE") >= 1)
// MAGIC    )

// COMMAND ----------

// MAGIC %md ### Manupulating Numeric Columns with Expressions

// COMMAND ----------

val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show(5)

// COMMAND ----------

df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

// COMMAND ----------

df.select(bround(col("UnitPrice"), 1).alias("roundedDown"), col("UnitPrice")).show(5) // bround rounds down if exactly in the middle

// COMMAND ----------

// MAGIC %md ## Correlation can be found either through function or through DF statistic method

// COMMAND ----------

df.stat.corr("Quantity", "UnitPrice")

// COMMAND ----------

df.select(corr("Quantity", "UnitPrice")).show()

// COMMAND ----------

val probs = Array(0.5)
val relError = 0.05
df.stat.approxQuantile("UnitPrice", probs, relError)

// COMMAND ----------

display(df.stat.crosstab("StockCode", "Quantity"))

// COMMAND ----------

// MAGIC %md ### Add new id

// COMMAND ----------

df.withColumn("id", monotonically_increasing_id()).show(10)

// COMMAND ----------

// MAGIC %md ### Working with Strings

// COMMAND ----------

df.select("Description").show(2, false)

// COMMAND ----------

df.select(initcap(col("Description"))).show(2, false)

// COMMAND ----------

df.select(lower(col("Description"))).show(2, false)

// COMMAND ----------

// MAGIC %md ### Functions for manipulating Whitespace
// MAGIC * lpad
// MAGIC * ltrim
// MAGIC * rpad
// MAGIC * rtrim
// MAGIC * trim

// COMMAND ----------

df.select(ltrim(lit("          Hello         ")).as("leftTrimmed"), rpad(lit("Hello"), 2, " ").as("rightPadded")).show(3)

// COMMAND ----------

// MAGIC %md ### Regular Expressions
// MAGIC * Spark makes use of Java Regular Expressions
// MAGIC * Some slight variations from other languages
// MAGIC * regexp_extract
// MAGIC * regexp_replace
// MAGIC * translate

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")
val regexString = simpleColors.map(_.toUpperCase).mkString("|") // pipe (|) signifies "or"

// COMMAND ----------

df.select(regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"), col("Description")).show(10, false)

// COMMAND ----------

df.select(translate(col("Description"), "LEET", "1337"), col("Description")).show(10, false)

// COMMAND ----------

// MAGIC %md ### Extracting Values

// COMMAND ----------

val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
df.select(regexp_extract(col("Description"), regexString, 1).as("color_clean"), col("Description")).show(10, false)

// COMMAND ----------

// MAGIC %md ### Check for existence

// COMMAND ----------

val containsBlack = lower(col("Description")).contains("black")
val containsWhite = lower(col("Description")).contains("white")
df.withColumn("hasSimpleColor", containsBlack.or(containsWhite)).where("hasSimpleColor").select("Description").show(10, false)

// COMMAND ----------

// MAGIC %sql -- It's different in SQL
// MAGIC select Description from dfTable where instr(Description, "BLACK") > 0 or instr(Description, "WHITE") > 0

// COMMAND ----------

// MAGIC %md ### Usually its more complicated than this

// COMMAND ----------

val simpleColors = Seq("black", "white", "red", "green", "blue")
val selectedColumns = simpleColors.map(color => { col("Description").contains(color.toUpperCase).as(s"is_$color") } ):+expr("*")
df.select(selectedColumns:_*).where(col("is_white").or(col("is_red"))).select("Description").show(10, false)

// COMMAND ----------

// MAGIC %md ### Working with Dates and Timestamps

// COMMAND ----------

val dateDF = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")

// COMMAND ----------

dateDF.printSchema

// COMMAND ----------

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(10, false)

// COMMAND ----------

dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show()

// COMMAND ----------

dateDF.select(to_date(lit("2016-01-01")).as("start"), to_date(lit("2017-05-22")).as("end")).select(months_between(col("start"), col("end"))).show

// COMMAND ----------

spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).printSchema

// COMMAND ----------

// MAGIC %md ### Set Date format manually to avoid parsing errors

// COMMAND ----------

val dateFormat = "yyyy-dd-MM"
val cleanDateDF = spark.range(1).select(
  to_date(lit("2017-12-11"), dateFormat).as("date"),
  to_date(lit("2017-20-12"), dateFormat).as("date2")
)
cleanDateDF.createOrReplaceTempView("dataTable2")
cleanDateDF.show

// COMMAND ----------

cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show

// COMMAND ----------

// return first non-null value from a set of columns
df.select(coalesce(col("Description"), col("CustomerId"))).show()

// COMMAND ----------

// MAGIC %md ### SQL has lots of functions for null values
// MAGIC * ifnull
// MAGIC * nullif
// MAGIC * nvl
// MAGIC * nvl2

// COMMAND ----------

df.na.drop("any").show // drops row if any value in that row is null

// COMMAND ----------

df.na.drop("all").show // drops row if all values in that row are null

// COMMAND ----------

df.na.drop("all", Seq("StockCode", "InvoiceNo")).show // apply to a subset of columns

// COMMAND ----------

// MAGIC %md ### Fill NA
// MAGIC * Specify a map

// COMMAND ----------

df.na.fill("All null values become this string").show

// COMMAND ----------

df.na.fill(5, Seq("StockCode", "InvoiceNo")).show // all null int values become 5 

// COMMAND ----------

df.printSchema

// COMMAND ----------

val meanPrice = df.select(mean("UnitPrice")).first()(0).toString.toDouble

// COMMAND ----------

df.na.fill(meanPrice, Seq("UnitPrice")).show

// COMMAND ----------

