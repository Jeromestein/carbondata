package org.apache.carbondata.benchmark

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.spark.util.DataGenerator
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random

object test extends App {
//  var totalNum = 10 * 1000 * 1000
//  val cardinalityId = 100 * 1000 * 1000
//  val cardinalityCity = 6
//  val r = new Random()
//  lazy val tmpId = r.nextInt(cardinalityId) % totalNum
//  lazy val tmpCity = "city" + (r.nextInt(cardinalityCity) % totalNum)
//  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    val master = Option(System.getProperty("spark.master"))
      .orElse(sys.env.get("MASTER"))
      .orElse(Option("local[8]"))
    val spark = SparkSession
      .builder()
      .master(master.get)
      .enableHiveSupport()
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")
    val df_1 = DataGenerator.generateDataFrame(spark, totalNum = 10 * 100).cache
    df_1.show(300)
////    val df_2 = spark.sparkContext
////      .parallelize(1 to 10000)
////      .map(x => ("city" + x % 6,r.nextInt(20), x % 300))
////      .toDF("city", "areacode", "population").cache
////    val df_3 = spark.sparkContext
////      .parallelize(1 to 10000)
////      .map(x => (x%50,x % 300))
////      .toDF("areacode","GDP per").cache
//
//  //将RDD to DF  DF to ORc ,spark 读ORC文件构建view
//    val formattime = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss")
//    val date = new Date()
//    print(formattime.format(date))
//    def function1(a:Int ,b:Int) = {
//        (a+b).toString + " this si funcitonality"
//    }
//    val function2 :String = function1(2,3)
//val queries: Array[Query] = Array(
//    // ===========================================================================
//    // ==                     FULL SCAN AGGREGATION                             ==
//    // ===========================================================================
//    Query(
//        "select sum(m1) from $table",
//        "full scan",
//        "full scan query, 1 aggregate"
//    ),
//    Query(
//        "select sum(m1), sum(m2) from $table",
//        "full scan",
//        "full scan query, 2 aggregate"
//    )
//)
//    queries.zipWithIndex.map { case (query, index) =>
//        val sqlText = query.sqlText.replace("$table", "RPLACE")
//        print(s"running query ${index + 1}: $sqlText ")
//    }
//    println("/")
//    val str = "this $replace"
//    print(str.replace("$replace","REPLACE"))
}
