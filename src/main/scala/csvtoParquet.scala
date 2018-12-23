package linear

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



object csvtoParquet {

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
    data.write.mode("overwrite").parquet("C:/alekya/files")

    val temp = loadTrainingData(spark)
    temp.toDF.createOrReplaceTempView("dataTBl")
    spark.sql("SELECT * FROM  dataTBl")
    temp.printSchema()
    temp.write.format("com.databricks.spark.avro").save("C:/alekya/files")


  }

  def loadTrainingData(spark:SparkSession):DataFrame = {
    val df = spark
      .read.format("csv")
      .option("header", "true")
      .load("C:/alekya/first.csv").na.drop()

    df
  }

  def TrainingData(spark:SparkSession):DataFrame ={
    val df =spark
      .read.format("parquet")
      .option("header", "true")
      .load("C:/alekya/spark-warehouse/files").na.drop()
    df
  }


}
