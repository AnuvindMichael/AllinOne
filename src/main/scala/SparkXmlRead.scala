object SparkXmlRead {
  import org.apache.spark._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import sys.process._
  import org.apache.log4j._
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

/*    val xmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///D:/Practice/book.xml")

    xmldf.show()
    xmldf.printSchema()*/

    print("==========READ CSV DATA USE DSL========")

    val df = spark.read.format("csv").option("header","true").option("delimiter",",").load("file:///D:/Practice/usdata.csv")
    df.show(2)

    val df2 = df.filter("state='LA'")
    df2.show(2)






  }

}
