package TestingPackage

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RefData {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val refDF = ReferenceDF(sqlContext)

    /*
        val refDF = sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("src\\SourceData\\REF_RML_ReferentielsMultiLangues.csv")
            .select("RML_PKCode","RML_Libelle1")

        refDF.write
            .partitionBy("RML_CodReferentiel")
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("delimiter", ";")
          .save("src\\GeneratedData\\RefData")
    */
  }

  def ReferenceDF(sqlContext: SQLContext): DataFrame = {

    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\REF_RML_ReferentielsMultiLangues.csv")
      .select("RML_PKCode", "RML_Libelle1")

  }
}


