package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object CustomerSolde {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    /*
      val customerDataDF = sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\GeneratedData\\CustomerRecord\\data.csv")

      val churnData = sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\GeneratedData\\CustomerChurn\\data.csv")

      val customerDF = customerDataDF.join(churnData, "IdClient")
       /*.select(
       Year =cal.get(Calendar.YEAR )

             "DatNaissanceOuCreation"
             "Agence"
             "SituationFamiliale"
             "profession"
             "Sexe"
             "PaysNaissance"
             "NbrOrdre"
             "NbrMail"
             "NbrTaches"
             "NbrAppel"
             "NbrRDV"
             "Churn"
          )*/

      customerDF.printSchema()*/

    val lienDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")

      //Changement des noms de colonnes
      .select(
      $"TCL_CodTiers".as("IdClient"),
      $"TCL_CodCompte_hash".as("IdCompte")
    )
    val soldeDetails = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\Soldes\\data.csv")

    val soldeClient = lienDF.join(soldeDetails,"IdCompte")
      .groupBy("IdClient")
      .agg(
        sum("SoldeEnEuro").as("SoldeEnEuro"),
        sum("SoldeTitres").as("SoldeTitres"))
      .withColumn("solde",$"SoldeEnEuro" + $"SoldeTitres" )

    soldeClient.printSchema()
    soldeClient
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\GeneratedData\\CustomerSolde")
  }
}


