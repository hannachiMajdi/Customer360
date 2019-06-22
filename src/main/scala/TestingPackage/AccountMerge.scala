package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object AccountMerge {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val soldeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("Soldes\\data.csv")

    val compteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")




      //Changement des noms de colonne
      .select($"GCO_CodCompte".as("IdCompte")
      , $"GCO_IsOuvert".as("IsOuvert")
      , $"GCO_IsAsv".as("IsAsv")
      , $"GCO_IsAssureur".as("IsAssureur")
      , $"GCO_IsPea".as("IsPea")
      , $"GCO_IsPeaPme".as("IsPeaPme")
      , $"GCO_CodProduit".as("IdProduit")
      , $"GCO_LibProduit".as("LibProduit")
      , $"GCO_LibEtatCompte".as("LibEtatCompte")
      , $"GCO_CodEnveloppeFiscale".as("CodEnveloppeFiscale")
      , $"GCO_CodTribu".as("CodContactCrm")
      , $"GCO_CodCapaciteSupportPertesMif2".as("CodCapaciteSupportPertes")
      , $"GCO_CodProfilInvestissementMif2".as("CodProfilInvestissement")
      , $"GCO_CodToleranceRisqueMif2".as("CodToleranceRisque")
      , $"GCO_CodHorizonPlacementMif2".as("CodHorizonPlacement")
      , $"GCO_CodObjectifPlacementMif2".as("CodObjectifPlacement")
      , $"GCO_CodProfilRisqueMif2".as("CodProfilRisque")
      , $"GCO_CodStrategieInvestissementMif2".as("CodStrategieInvestissement")
      , $"GCO_AllocationActionMinMif2".as("AllocationActionMin")
      , $"GCO_AllocationActionMaxMif2".as("AllocationActionMax")
      , $"GCO_IsNanti".as("IsNanti"))

    // La liste des produits

    compteDF.select("IdProduit","LibProduit").distinct()
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("Produits")

    // la liste des comptes
    compteDF.distinct()
      .join(soldeDF.distinct(),"IdCompte")
      .repartition(1)
    .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("Comptes")


  }
}


