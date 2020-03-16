// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 5: Basic Structured Operations

// COMMAND ----------

// MAGIC %md All operations are translated and performed on *Spark* types, not Scala or Python types.

// COMMAND ----------

val flightData2015 = spark.read.format("json").load("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/flight-data/json/2015-summary.json")

// COMMAND ----------

flightData2015.printSchema

// COMMAND ----------

flightData2015.schema

// COMMAND ----------

val myManualSchema = StructType( Array (
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false, Metadata.fromJson("{\"hello\": \"world\"}"))
) )

val flightData2015 = spark.read.format("json").schema(myManualSchema).load("s3://databricks-cc/jeremy/spark-the-definitive-guide/data/flight-data/json/2015-summary.json")

// COMMAND ----------

flightData2015.printSchema

// COMMAND ----------

flightData2015.schema

// COMMAND ----------

flightData2015.columns

// COMMAND ----------

flightData2015.first

// COMMAND ----------

val myRow = Row("Hello", null, 1, false)

// COMMAND ----------

myRow(0) // type any

// COMMAND ----------

myRow(0).asInstanceOf[String] // string

// COMMAND ----------

myRow.getString(0) // string

// COMMAND ----------

myRow.getInt(2) // int

// COMMAND ----------

flightData2015.selectExpr("DEST_COUNTRY_NAME as newColumn", "DEST_COUNTRY_NAME").show(10)

// COMMAND ----------

flightData2015.selectExpr("*", "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry").show(50)

// COMMAND ----------

flightData2015.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(20)

// COMMAND ----------

flightData2015.createOrReplaceTempView("flights")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC     avg(count), 
// MAGIC     count(distinct(DEST_COUNTRY_NAME))
// MAGIC from flights

// COMMAND ----------

// pass literals to add a column of all one value
flightData2015.select(expr("*"), lit(1).as("One")).show(2)

// COMMAND ----------

flightData2015.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(5)

// COMMAND ----------

flightData2015.withColumn("count2", col("count").cast("double")).printSchema

// COMMAND ----------

flightData2015.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(5)

// COMMAND ----------

// MAGIC %md ## Random Samples

// COMMAND ----------

val seed = 5
val withReplacement = false
val fraction = 0.5
flightData2015.sample(withReplacement, fraction, seed).count

// COMMAND ----------

flightData2015.count

// COMMAND ----------

val dataFrames = flightData2015.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count > dataFrames(1).count

// COMMAND ----------

val schema = flightData2015.schema
val newRows = Seq(
  Row("New Country", "Other Country", 5L),
  Row("Newer Country", "Otherer Country", 1L)
)
val pRows = sc.parallelize(newRows)
val newDF = spark.createDataFrame(pRows, schema)
flightData2015.union(newDF).where("count = 1").where($"ORIGIN_COUNTRY_NAME" =!= "United States").show(50)

// COMMAND ----------

display(flightData2015.sort(desc("count")))

// COMMAND ----------

display(flightData2015.sort(asc_nulls_first("count")))

// COMMAND ----------

display(flightData2015.sortWithinPartitions("count"))

// COMMAND ----------

flightData2015.rdd.getNumPartitions

// COMMAND ----------

flightData2015.repartition(5).rdd.getNumPartitions // creates a new object

// COMMAND ----------

flightData2015.repartition(5, col("DEST_COUNTRY_NAME"))

// COMMAND ----------

flightData2015.repartition(5).coalesce(2).rdd.getNumPartitions

// COMMAND ----------

flightData2015.take(5) // brings first 5 rows to driver

// COMMAND ----------

flightData2015.collect() // brings entire dataframe to driver

// COMMAND ----------

// MAGIC %md ## To bring rows to driver in the form of an interator:

// COMMAND ----------

val iter = flightData2015.toLocalIterator()

// COMMAND ----------

iter.next

// COMMAND ----------

iter.next

// COMMAND ----------

