import spire.implicits.eqOps
import spire.optional.genericEq.generic

object SparkParttionBy {
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
/*    val df = spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",").load("file:///D:/Practice/usdata.csv")
    df.show(10)

    df.write.format("json").partitionBy("state","county").mode("overwrite")
      .save("file:///D:/Practice/partitionBydata")*/

/*    val df2 = spark.read.format("csv")
      .options(Map("header"->"true","delimiter"->"~"))
      .load("file:///D:/Practice/txns_head")
      df2.show()

    val df3 = df2.filter(col("category" )=== "Gymnastics")
    df3.show(10)

    val df4 = df2.filter(col("category") === "Gymnastics" && col("spendby")==="cash")
    df4.show(10)

    val df5 = df2.filter(col("category").isin("Gymnastics","Team Sports"))
    df5.show(10)

    val df6 = df2.filter(!col("category") === "Gymnastics")
    df6.show(10)

    val df7 = df2.filter(col("category") === "Gymnastics"
                    &&
                    col("product") .like("%Gymnastics%") )
    df7.show(10)*/

    val df8 = spark.read.format("json")
      .load("file:///D:/Practice/null.json")
    df8.show()

    val df9 = df8.filter(col("name").isNull)
    df9.show()

    val df10 = df8.filter(!col("name").isNull)
    df10.show()











  }

}
