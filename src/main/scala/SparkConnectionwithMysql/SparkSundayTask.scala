package SparkConnectionwithMysql

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._
import sys.process._

object SparkSundayTask {
  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").load("file:///D:/Practice/devices.json")
    //df.show(5)
    val df1 = df.filter(col("lat")> 70)
    df1.show(4)
    df1.write.format("parquet").mode("overwrite").save("file:///D:/Practice/Sundaytask")
    print("==========Data write completed===========")






  }



}
