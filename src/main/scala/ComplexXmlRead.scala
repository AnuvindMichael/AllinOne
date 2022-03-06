object ComplexXmlRead {

  import org.apache.spark._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import sys.process._
  import org.apache.log4j._
    def main(args:Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._

/*
      println()
      print("===========Complex XML Data Read============")
      println()

      val xmlDf = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog")
        .load("file:///D:/Practice/complexxml.xml")

      xmlDf.show()
      xmlDf.printSchema()
*/

      println()
      print("===========USData Read============")
      println()

      val csdf = spark.read.format("csv")
              .options(Map("header"->"true","delimiter"->",","inferSchema"->"true"))
        .load("file:///D:/Practice/usdata.csv")

      csdf.show(10)

      val filterdata = csdf.filter("state = 'LA'")
      filterdata.show(2)

      filterdata.select("first_name","last_name").show(2)


    }
}



