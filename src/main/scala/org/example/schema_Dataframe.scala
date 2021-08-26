package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

object schema_Dataframe {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc1: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Schema")
      .getOrCreate()
    sc1.sparkContext.setLogLevel("ERROR")

    val namesDf= sc1.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("src/main/resources/user.csv")
    namesDf.printSchema()

    val userschema =StructType(
                    StructField("userid",LongType,true) ::
                      StructField("mailid",StringType,true) ::
                      StructField("language",StringType,true) ::
                      StructField("country",StringType,true) :: Nil)
    val ownDf= sc1.read
      .option("header","true")
      .schema(userschema)
      .csv("src/main/resources/user.csv")

    ownDf.printSchema()
  }
}