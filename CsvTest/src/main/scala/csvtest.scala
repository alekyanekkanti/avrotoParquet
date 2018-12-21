package com.basic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset
import java.time.chrono.ChronoLocalDate

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.{LongType, StructField, StructType};

object csvtest {

//  case class Flight (
//                     Year : String,
//                     Month : String,
//                    DayofMonth :String,
//                    CRSDepTime : String,
//                    CRSArrTime : String,
//                    CRSElapsedTime : String,
//                     UniqueCarrier : String,
//                     FlightNum : String,
//                     TailNum :String,
//                     Origin: String,
//                     Distance: Double,
//                     ArrDelay: Double )
//
//   case class wheather (
//                         TmaxF :String,
//                        TminF :String,
//                        TmeanF :String,
//                        PrcpIn : String,
//                        SnowIn :String,
//                        CDD :String,
//                        HDD :String,
//                        GDD :String,
//                        date: String)

  def main(args: Array[String]) ={

    val spark = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()
    //sql context
    //val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
    import spark.implicits._



    val wrawdata=spark
      .read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/alekya/Chicago_Ohare_International_Airport.csv")
      .repartition(6).toDF()

    val airlinesTable1=spark
      .read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:/alekya/allyears2k_headers.csv")
      .repartition(6).toDF()
    import spark.implicits._

//    airlinesTable1.printSchema()
//    wrawdata.printSchema()

    val airRDD = airlinesTable1.toDF()
    val wheatRDD = wrawdata.toDF()

    //airRDD.show()
    //wheatRDD.show()



    //spark.sql("SELECT * FROM global_temp.FligthsToORD").show()

    airlinesTable1.toDF.createOrReplaceTempView("FlightsToORD")
    wrawdata.toDF.createOrReplaceTempView("WeatherORD")




    //val kk= sqlContext.sql("select TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date,'dd/MM/yyyy'),'yyyy-MM-dd')) AS dt, w.date from WeatherORD w")

    //register UDF function
    spark.udf.register("ksn",dateStr _)

    val bigTable = spark.sql(
      """SELECT
        |f.Year,f.Month,f.DayofMonth,
        |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
        |f.UniqueCarrier,f.FlightNum,f.TailNum,
        |f.Origin,f.Distance,
        |w.TmaxF,w.date as temp_date, w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
        |f.ArrDelay
        |FROM FlightsToORD f
        |JOIN WeatherORD w
        |ON
        |TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(w.date,'dd/MM/yyyy'),
        |'yyyy-MM-dd')) = TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(ksn(f.Year,f.Month,f.DayofMonth),'yyyyMMdd'),'yyyy-MM-dd'))
        |WHERE f.ArrDelay IS NOT NULL""".stripMargin)

    bigTable.printSchema()
    bigTable.show()


  }
  def dateStr(yr:Int, mon:Int, d:Int):String = {
      return yr+""+mon+""+d

  }
}
