package DWPackage

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object TestingElasticSearch {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ToGraphMigration")


    val sc = new SparkContext(conf)


    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataDF =
      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CRO_CRO_CROD.csv")

    //dataDF.saveToEs("test/HelloES")
    dataDF.printSchema()
    dataDF.describe().show()
  }



}


