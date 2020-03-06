// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 3: A Gentle Introduction to Spark

// COMMAND ----------

val dataPath = "s3://databricks-cc/jeremy/spark-the-definitive-guide/data/2015-summary.csv"
val flightData2015 = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)
flightData2015.printSchema

// COMMAND ----------

display(flightData2015)

// COMMAND ----------

flightData2015.take(3)

// COMMAND ----------

flightData2015.sort("count").explain()

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "10")
flightData2015.sort("count").take(2)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")
flightData2015.sort("count").take(2)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "50")
flightData2015.sort("count").take(2)

// COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")

// COMMAND ----------

val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, COUNT(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")
sqlWay.explain

// COMMAND ----------

val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
dataFrameWay.explain

// COMMAND ----------

// MAGIC %md This allows you take advantage of a wider set of manipulations (SQL or DataFrame) 

// COMMAND ----------

flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).show

// COMMAND ----------

val maxSql = spark.sql("""
select DEST_COUNTRY_NAME, sum(count) as destination_total
from flight_data_2015
group by DEST_COUNTRY_NAME
order by sum(count) desc
limit 5
""")

maxSql.show

// COMMAND ----------

maxSql.explain

// COMMAND ----------

flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).explain

// COMMAND ----------

// MAGIC %md ## Spark Streaming with Retail Data

// COMMAND ----------

// static version of data
val staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/retail-data/by-day/*.csv")

// COMMAND ----------

staticDataFrame.printSchema

// COMMAND ----------

staticDataFrame.createOrReplaceTempView("retail_data")

// COMMAND ----------

val staticSchema = staticDataFrame.schema

// COMMAND ----------

display(staticDataFrame)

// COMMAND ----------

display(staticDataFrame.describe())

// COMMAND ----------

staticDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
  .groupBy(col("customerId"), window(col("InvoiceDate"), "1 day"))
  .sum("total_cost")
  .orderBy(desc("sum(total_cost)"))
  .show(5, false)

// COMMAND ----------

// streaming version of data
val streamingDataFrame = spark.readStream.format("csv").schema(staticSchema).option("header", "true").option("maxFilesPerTrigger", 1)
  .load("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/retail-data/by-day/")

// COMMAND ----------

streamingDataFrame.isStreaming

// COMMAND ----------

val purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
  .groupBy($"customerId", window($"InvoiceDate", "1 day"))
  .sum("total_cost")

// COMMAND ----------

// action we call on streaming dataframe will output to an in-memory table after each *trigger*

purchaseByCustomerPerHour.writeStream.format("memory") // store in in-memory table
  .queryName("customer_purchases")                     // the name of the in-memory table
  .outputMode("complete")                              // complete = all the counts should be in the table
  .start()

// COMMAND ----------

spark.sql("""
select *
from customer_purchases
order by 'sum(total_cost)' desc
""")
.show(5)

// COMMAND ----------

spark.sql("""
select *
from customer_purchases
order by 'sum(total_cost)' desc
""")
.show(5)

// COMMAND ----------

// could also write results out to console
purchaseByCustomerPerHour.writeStream.format("console") // store in in-memory table
  .queryName("customer_purchases_2")                     // the name of the in-memory table
  .outputMode("complete")                              // complete = all the counts should be in the table
  .start()