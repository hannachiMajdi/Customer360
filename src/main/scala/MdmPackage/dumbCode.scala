package MdmPackage

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object dumbCode {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val BridgeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-f7a024e9-7641-452b-afd2-57e21469d1b0-c000.csv")


    val InDF =  sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_interaction\\part-00000-d1f3496e-b823-46d6-be15-fec204ead6dd-c000.csv")

    val TrDF =  sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_transaction\\part-00000-f39a60be-c38c-41e2-b585-81b097db8988-c000.csv")
      .select("FK_CodCompte").distinct()

    val SdDF =  sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_soldeactivity\\part-00000-2c90d107-75dc-4d66-a6fb-23d88212d261-c000.csv")

      .select("FK_CodCompte").distinct()

    val CpDF =  sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_compteactivity\\part-00000-60cdb97e-89ce-4bd3-b2cc-1c18a7cd9380-c000.csv")
      .select("FK_CodCompte").distinct()

    val CustomerDf = BridgeDF
      .join(TrDF,"FK_CodCompte")
      .join(SdDF,"FK_CodCompte")
      .join(CpDF,"FK_CodCompte")
      .select("FK_CodTiers").distinct()

    CustomerDf
      //.saveToEs("dw_dimension_client/client")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("SourceData\\client")
  }


}


