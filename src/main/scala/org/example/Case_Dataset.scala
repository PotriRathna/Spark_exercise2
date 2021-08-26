package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class Language(userid:Integer, mailid: String, language:String,country:String)

object Case_Dataset {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc1: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("case")
      .getOrCreate()
    sc1.sparkContext.setLogLevel("ERROR")

    import sc1.implicits._
    val namesDf= sc1.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/user.csv")
      .as[Language]
    namesDf.printSchema()
    namesDf.show()

    val filterlang = namesDf.filter(x => x.language=="English" )
    filterlang.show()
    println(filterlang.count())
    val wheredemo = namesDf.where(namesDf("country") === "India")
    wheredemo.show()
    val selectcolumn = namesDf.select("userid","mailid")
    selectcolumn.show()

  }
}
