package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object SoldeDetails {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    val histoExtraitDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\data\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")

      //Suppression des autres colonnes
      .drop("PGH_CodSociete")



      //Changement des noms de colonnes
      .withColumnRenamed("PGH_CodCompte", "IdCompteS")
      .withColumnRenamed("PGH_DatArrete", "DateExtraitS")
      .withColumnRenamed("PGH_MntTotalValorisationTitresEnEuros", "SoldeTitres")
      .withColumnRenamed("PGH_MntSoldeEuroEnEuros", "SoldeEnEuro")

    val datesDF = histoExtraitDF.select("IdCompteS", "DateExtraitS")
      .groupBy("IdCompteS").agg(max("DateExtraitS") as "DateExtrait")
      .distinct()
      .withColumnRenamed("IdCompteS", "IdCompte")


    val soldesDF = histoExtraitDF
      .join(datesDF,
        (datesDF("IdCompte") === histoExtraitDF("IdCompteS")) && (datesDF("DateExtrait") === histoExtraitDF("DateExtraitS"))
        , "inner")
      .distinct()



    /*
  soldesDF
    .select("IdCompte", "DateExtrait", "SoldeEnEuro", "SoldeTitres")
    .repartition(1)
    .write
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("delimiter", ";")
    .save("Soldes")

*/
  }
}


