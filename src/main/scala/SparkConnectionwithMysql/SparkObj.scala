package SparkConnectionwithMysql
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import sys.process._
import org.apache.log4j._
object SparkObj {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
   sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sqldf = spark.read.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
      .option("dbtable","cashdata")
      .option("user","root")
      .option("password","Aditya908")
      .load()

    sqldf.show()

    sqldf.createOrReplaceTempView("sqltable")

    val df2 = spark.sql("select * from sqltable where category = 'Gymnastics'")
    df2.show()


    df2.write.format("com.databricks.spark.avro").mode("append").save("file:///D:/Practice/saturdaytask")

    print("=====AVRO DATA COMPLETED=========")

       sqldf.write.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://zeyodb.chwk6wgpdls3.ap-south-1.rds.amazonaws.com:3306/zeyodata")
      .option("dbtable","rdstable")
      .option("user","root")
      .option("password","Aditya908")
      .save()

    print("=============mysql table exported==========")




  }

}
