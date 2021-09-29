package org.apache.carbondata.benchmark

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.spark.util.DataGenerator
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

object SubqueryBenchmark {
  def parquetTableName: String = "comparetest_parquet"
  def orcTableName: String = "comparetest_orc"
  def carbonTableName(version: String): String = s"comparetest_carbonV$version"

  val queries: Array[Query] = Array(
    Query(
      "select id from (select id from $table where m5 between 12 and 18)",
      "full scan",
      "from subquery on high cardinality dimension"
    ),
    Query(
      "select id from (select id from $table where m1 between 2 and 10)",
      "full scan",
      "from subquery on low cardinality dimension"
    ),
    Query(
      "select id from (select id from $table where m2 between 50 and 100)",
      "full scan",
      "from subquery on medium cardinality dimension"
    ),
    Query(
      "select id from $table where id < (select id from $table where m5 between 12 and 18)",
      "full scan",
      "where subquery on high cardinality dimension"
    )
//    Query(
//      "select id from $table where id < (select DISTINCT id from $table where m1 between 2 and 10 )",
//      "full scan",
//      "where subquery on low cardinality dimension"
//    )
//    Query(
//      "select id from $table where id < (select id from $table where m3 between 50 and 100)",
//      "full scan",
//      "where subquery on medium cardinality dimension"
//    )
  )
  private def loadParquetTable(spark: SparkSession, input: DataFrame, table: String)
  : Double = time {
    // partitioned by last 1 digit of id column
    val dfWithPartition = input.withColumn("partitionCol", input.col("id").%(10))
    // DF to parquet ranhy
    dfWithPartition.write
      .partitionBy("partitionCol")
      .mode(SaveMode.Overwrite)
      .parquet(table)
    spark.read.parquet(table).createOrReplaceTempView(table)
  }

  private def loadOrcTable(spark: SparkSession, input: DataFrame, table: String): Double = time {
    // partitioned by last 1 digit of id column
    //DF to ORc ,spark 读ORC文件构建view表
    input.write
      .mode(SaveMode.Overwrite)
      .orc(table)
    spark.read.orc(table).createOrReplaceTempView(table)
  }

  private def loadCarbonTable(spark: SparkSession, input: DataFrame, tableName: String): Double = {
    //文件版本
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATA_FILE_VERSION,
      "V3"
    )
    //DF to carbondata表
    spark.sql(s"drop table if exists $tableName")
    time {
      input.write
        .format("carbondata")
        .option("tableName", tableName)
        .option("table_blocksize", "32")
        .mode(SaveMode.Overwrite)
        .save()
    }
  }

  // load data into parquet, carbonV2, carbonV3
  private def prepareTable(spark: SparkSession, table1: String, table2: String): Unit = {
    val df = DataGenerator.generateDataFrame(spark, totalNum = 10 * 2).cache
    println(s"loading ${df.count} records, schema: ${df.schema}")
    val table1Time = if (table1.endsWith("parquet")) {
      loadParquetTable(spark, df, table1)
    } else if (table1.endsWith("orc")) {
      loadOrcTable(spark, df, table1)
    } else {
      sys.error("invalid table: " + table1)
    }
    val table2Time = loadCarbonTable(spark, df, table2)
    println(s"load completed, time: $table1Time, $table2Time")
    df.unpersist()
  }

  // Run all queries for the specified table
  private def runQueries(spark: SparkSession, tableName: String): Array[(Double, Array[Row])] = {
    println(s"start running queries for $tableName...")
    var result: Array[Row] = null
    queries.zipWithIndex.map { case (query, index) =>
      val sqlText = query.sqlText.replace("$table", tableName)
      print(s"running query ${index + 1}: $sqlText ")
      val rt = time {
        result = spark.sql(sqlText).collect()
      }
      println(s"=> $rt sec")
      (rt, result)
    }
  }

  private def printErrorIfNotMatch(index: Int, table1: String, result1: Array[Row],
                                   table2: String, result2: Array[Row]): Unit = {
    // check result size instead of result value, because some test case include
    // aggregation on double column which will give different result since carbon
    // records are sorted
    if (result1.length != result2.length) {
      val num = index + 1
      println(s"$table1 result for query $num: ")
      println(s"""${result1.mkString(",")}""")
      println(s"$table2 result for query $num: ")
      println(s"""${result2.mkString(",")}""")
      sys.error(s"result not matching for query $num (${queries(index).desc})")
    }
  }

  // run test cases and print comparison result
  private def runTest(spark: SparkSession, table1: String, table2: String): Unit = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date
    // run queries on parquet and carbon
    val table1Result: Array[(Double, Array[Row])] = runQueries(spark, table1)
    // do GC and sleep for some time before running next table
    System.gc()
    Thread.sleep(1000)
    System.gc()
    Thread.sleep(1000)
    val table2Result: Array[(Double, Array[Row])] = runQueries(spark, table2)
    // check result by comparing output from parquet and carbon
    table1Result.zipWithIndex.foreach { case (result, index) =>
      printErrorIfNotMatch(index, table1, result._2, table2, table2Result(index)._2)
    }
    // print all response time in JSON format, so that it can be analyzed later
    queries.zipWithIndex.foreach { case (query, index) =>
      println("{" +
        s""""query":"${index + 1}", """ +
        s""""$table1 time":${table1Result(index)._1}, """ +
        s""""$table2 time":${table2Result(index)._1}, """ +
        s""""fetched":${table1Result(index)._2.length}, """ +
        s""""type":"${query.queryType}", """ +
        s""""desc":"${query.desc}",  """ +
        s""""date": "${formatter.format(date)}" """ +
        "}"
      )
    }
  }
  def main(args: Array[String]): Unit = {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    import org.apache.spark.sql.CarbonSession._
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark/target/store"
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
    spark.sparkContext.setLogLevel("warn")

    val table1 = parquetTableName
    val table2 = carbonTableName("3")
    prepareTable(spark, table1, table2)
    runTest(spark, table1, table2)

    CarbonUtil.deleteFoldersAndFiles(new File(table1))
    spark.sql(s"drop table if exists $table2")
    spark.close()
  }

  def time(code: => Unit): Double = {
    val start = System.currentTimeMillis()
    code
    // return time in second
    (System.currentTimeMillis() - start).toDouble / 1000
  }
}
