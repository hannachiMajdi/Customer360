package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object TestH {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)

      val extraitDF= sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",",")
        .option("inferSchema", "true")
        .load("src\\SourceData\\Churn_Modelling.csv")
        .withColumnRenamed("RowNumber","Index")

      val amedDF= sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\amed.csv")

      val jointureDF = amedDF.join(extraitDF,"Index")


      jointureDF.printSchema()
      jointureDF
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",";")
        .save("Ahmed")
    }
}


