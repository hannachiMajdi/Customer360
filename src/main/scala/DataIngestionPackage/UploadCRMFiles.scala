package DataIngestionPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object UploadCRMFiles {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Upload CRM Files").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)


      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLakeDemo/CRM/Contact")

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CRM_V_INTERACTIONSOBP.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLakeDemo/CRM/Interaction")

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\ContactRelation_OBP_CRM.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLakeDemo/CRM/Relation")

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\names\\names.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLakeDemo/Other/Names")

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\DimDates.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLakeDemo/Other/DimDates")




    }
}


