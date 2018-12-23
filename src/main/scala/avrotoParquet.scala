package com.databricks.spark.avro


import scala.xml._
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
 import scala.collection.JavaConverters._
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger}
//import org.apache.spark.sql.avro._


object avrotoParquet {



  def main(args: Array[String]): Unit = {


    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    import spark.implicits._

    val data = loadTrainingData(spark)
    data.toDF.createOrReplaceTempView("dataTBl")
    spark.sql("SELECT * FROM  dataTBl")
    data.printSchema()
    data.write.mode("overwrite").parquet("C:/alekya/files/_temporary/0/task_20181223122824_0001_m_000000.parquet")

    val temp = TrainingData(spark)
    temp.toDF.createOrReplaceTempView("dataTBl")
    spark.sql("SELECT * FROM  dataTBl")
    temp.printSchema()
    temp.write.format("com.databricks.spark.avro").save("C:/alekya/avro")

    val first = Data(spark)
    first.toDF.createOrReplaceTempView("dataTBl")
    spark.sql("SELECT * FROM  dataTBl")
    first.printSchema()
    first.write.format("json").save("C:/alekya/files/example.json")

    val two = Data1(spark)
    two.toDF.createOrReplaceTempView("dataTBl")
    spark.sql("SELECT * FROM  dataTBl")
    two.printSchema()
    two.write.format("com.databricks.spark.xml").save("C:/alekya/files/first.xml")

    val three = Data2(spark)
    three.toDF.createOrReplaceTempView("dataTBl")
    val ten = spark.sql("SELECT * FROM  dataTBl WHERE productID > 3")
    ten.printSchema()
    ten.write.format("csv").save("C:/alekya/files/second.csv")

    val result = data.except(ten)
     result.toDF().show()
    val result1 = ten.except(data)
    result1.toDF().show()

  }

  def loadTrainingData(spark:SparkSession):DataFrame = {
    val df1 = spark
      .read.format("csv")
      .option("header", "true")
      .load("C:/alekya/first.csv").na.drop()

    df1
  }

  def TrainingData(spark:SparkSession):DataFrame ={
    val df2 =spark
      .read.format("parquet")
      .option("header", "true")
      .load("C:/alekya/files/_temporary/0/task_20181223122824_0001_m_000000.parquet").na.drop()
    df2
  }
  def Data(spark:SparkSession):DataFrame ={
    val df3 =spark
      .read.format("com.databricks.spark.avro")
      .load("C:/alekya/avro/").na.drop()
    df3.printSchema()
    df3

  }
  def Data1(spark:SparkSession):DataFrame = {
    val df4 = spark
      .read.format("json")
      .load("C:/alekya/files/example.json").na.drop()
    df4.printSchema()
    df4

  }

  def Data2(spark:SparkSession):DataFrame = {
    val df5 = spark
      .read.format("com.databricks.spark.xml")
      .load("C:/alekya/files/first.xml").na.drop()
    df5.printSchema()
    df5

  }
}

