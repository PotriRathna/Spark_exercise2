package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract,desc}
object Logfile_example {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LogFile")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val logDf= spark.read.text("src/main/resources/weblog.csv")
    logDf.printSchema()
    logDf.show(4,false)

    // parsing the Log File
    val parse_logdf = logDf.select(regexp_extract($"value","""^([^(\s|,)]+)""",1).alias("host"),
      regexp_extract($"value","""^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("timestamp"),
      regexp_extract($"value","""^.*\w+\s+([^\s]+)\s+HTTP.*""",1).as("path"),
      regexp_extract($"value","""^.*,([^\s]+)$""",1).cast("int").alias("status"))
      parse_logdf.show(4,false)
      parse_logdf.printSchema()
    // Frequent Hosts
    parse_logdf.groupBy("host").count().filter($"count" > 10).show()
    // status column statistics
    parse_logdf.describe("status").show()
    // HTTP status analysis
    parse_logdf.groupBy("status").count().sort("status").show()

   //    Visualizing Paths
  parse_logdf .groupBy("path").count().sort(desc("count")).show()
     //  Top Paths
    parse_logdf.groupBy("path").count().sort(desc("count")).show(10)
    // Top Ten Error Paths
   parse_logdf.filter($"status" =!= 200).groupBy("path").count().sort(desc("count"))
      .show(10)
      //Number of unique Hosts
    val unique_host_count = parse_logdf.select("host").distinct().count()
    println("Unique hosts : %d".format(unique_host_count))

  }
}
