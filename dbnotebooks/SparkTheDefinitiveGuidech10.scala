// Databricks notebook source
// MAGIC %md # Spark: The Definitive Guide

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

// MAGIC %md ## Chapter 9: Data Sources
// MAGIC * Six core data sources
// MAGIC  * CSV
// MAGIC  * JSON
// MAGIC  * Parquet
// MAGIC  * ORC
// MAGIC  * JDBC/ODBC connections
// MAGIC  * Plain-tex files
// MAGIC * Some community created sources
// MAGIC  * Cassandra
// MAGIC  * HBase
// MAGIC  * MongoDB
// MAGIC  * AWS Redshift
// MAGIC  * XML
// MAGIC  * ...

// COMMAND ----------

// MAGIC %md ## Read API Structure
// MAGIC * DataFrameReader.format(...).option("key", "value").schema(...).load(path)
// MAGIC * spark.read is shorthand for DataFrameReader

// COMMAND ----------

// MAGIC %md ## Read Modes
// MAGIC * permissive (default): sets all fields to null when it encounters a corupted record; places all corrupted records in a string column called _corrupt_record
// MAGIC * dropMalformed: drops any row with any malformed data
// MAGIC * failFast: fails immediately if it encounters malformed data

// COMMAND ----------

// MAGIC %md ## Write API Structure
// MAGIC * DataFrameWriter.format(...).option("key", "value").partitionBy(...).bucketBy(...).sortBy(...).save(path)
// MAGIC * df.write is shorthand for DataFrameWriter

// COMMAND ----------

// MAGIC %md ## Save Modes
// MAGIC * append: appends output files to list of files already at the location
// MAGIC * overwrite: erase, then write
// MAGIC * erorIfExists: fails if any data or files already exist at the location
// MAGIC * ignore: if data or files exist already at the location, do nothing

// COMMAND ----------

