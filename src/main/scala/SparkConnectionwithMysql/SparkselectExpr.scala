package SparkConnectionwithMysql

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j._
import spire.implicits.eqOps

import sys.process._
object SparkselectExpr {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv")
                  .options(Map("header"->"true","delimiter"->"~","inferSchema"->"true"))
                  .load("file:///D:/Practice/txns_head.csv")
    df.show(5)

    val df1 = df.selectExpr(
                          "txnno",
                                "split(txndate,'-')[1] as Month",
                                 "custno",
                                 "amount",
    "category", "product","city","state","spendby"
    )

    df1.show(5)

    val df2 = df.filter(col("category")==="Gymnastics")
    df2.show(5)

    val df3 = df2.withColumn("txndate",expr("split(txndate,'-')[1]"))
    df3.show(5)

    val df4 = df3.withColumn("status",expr("case when spendby='cash' then 1 else 0 end " ))
    df4.show(10)










  }

}
