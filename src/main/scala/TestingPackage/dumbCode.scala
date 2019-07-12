package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



object dumbCode {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val customerDF =
    sqlContext.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
    customerDF.printSchema()



  }
}


