package DataIngestionPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object UploadCoreBankingFiles {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Upload Core Banking Files").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)


    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/Tiers")

    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/Comptes")

    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\MEN_GCO_GeneriquesComptes.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/HistoCompte")


    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CRO_CRO_CROD.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/Transaction")

    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\INS_GIN_GeneriqueInstruments.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/Instruments")

    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\HIS_POH_PositionHisto__extrait.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/Extrait")

    sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("hdfs://localhost:9000/DataLake/CoreBanking/HistoExtrait")


    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("hdfs://localhost:9000/DataLake/CoreBanking/TiersCompte")

    }
}


