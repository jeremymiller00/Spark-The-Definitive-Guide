// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 8: Joins
// MAGIC * Joins expression determines if two rows should join
// MAGIC * Join type determines what should be in the result set

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
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show

// COMMAND ----------

// MAGIC %md ## Semi-joins and Anit-joins function like filters
// MAGIC * Semi-join will keep values in the left dataframe if the key exists in the right dataframe
// MAGIC * Anti-join will keep values in the left dataframe if the key does not exist in the right dataframe

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
val joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show

// COMMAND ----------

// MAGIC %md ## Natural Joins make an implicit guess about what to join: DANGER!  
// MAGIC Cross (Cartesian) joins create the cartesian product
// MAGIC * Every row in left combined with every row in right
// MAGIC * Gets big very fast!

// COMMAND ----------

// MAGIC %md ## Joins on complex types
// MAGIC * Any expression which evaluates to a boolean is a valid join expression

// COMMAND ----------

person.withColumnRenamed("id", "personId").join(sparkStatus, expr("array_contains(spark_status, id)")).show

// COMMAND ----------

// MAGIC %md ## Handling duplicate column names
// MAGIC * Option 1: different join expression
// MAGIC * Option 2: dropping the column after the join
// MAGIC * Option 3: rename a column before the join

// COMMAND ----------

val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
val joinExpr = gradProgramDupe.col("graduate_program") === person.col("graduate_program")
person.join(gradProgramDupe, joinExpr).show

// COMMAND ----------

person.join(gradProgramDupe, joinExpr).select("graduate_program").show

// COMMAND ----------

// different join expression: change join expression to string, automaigaclly removes one of the columns
person.join(gradProgramDupe, "graduate_program").show

// COMMAND ----------

// drop a column after the join
person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program")).show

// COMMAND ----------

// MAGIC %md ## How Spark performs joins
// MAGIC * Node-to-node communication strategy
// MAGIC  * Shuffle join -> big table to big table, all to all communication, can be expensive if data not partitioned appropriately (key step!)
// MAGIC  * Brodcast join -> big table to small table, replicate small table on every worker node to execute join, no internode communication required, cpu is biggest bottleneck
// MAGIC * Per node computation strategy

// COMMAND ----------

val joinExpr = graduateProgram.col("id") === person.col("graduate_program")
person.join(graduateProgram, joinExpr).explain

// COMMAND ----------

// can 'ask' catalyst for a broadcast join
person.join(broadcast(graduateProgram), joinExpr).explain

// COMMAND ----------

// MAGIC %md ## Conclusion: smart partitioning can increase join efficiency

// COMMAND ----------

