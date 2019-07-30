package MdmPackage

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MasterCompteBK {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataDF =

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\HIS_POH_PositionHisto__extrait.csv")
     .select(
        $"POH_CodCompte".as("CodCompte"),
        $"POH_DatArrete".as("DateExtrait"),
        $"POH_MntValorisationTitresEnEuros".as("SoldeTitres"),
        $"POH_QteTitres".as("SoldeEnEuro")
      )
          .unionAll(
            sqlContext.read.format("csv")
              .option("header", "true")
              .option("delimiter", ";")
              .option("inferSchema", "true")
              .load("src\\SourceData\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")
              .select(
                $"PGH_CodCompte".as("CodCompte"),
                $"PGH_DatArrete".as("DateExtrait"),
                $"PGH_MntTotalValorisationTitresEnEuros".as("SoldeTitres"),
                $"PGH_MntSoldeEuroEnEuros".as("SoldeEnEuro")
          )
          )

    val datesDF = dataDF.select("CodCompte", "DateExtrait")
      .groupBy("CodCompte").agg(max("DateExtrait") as "DateExtraitS")
      .distinct()
      .withColumnRenamed("CodCompte", "CodComptes")


    val soldesDF = dataDF
      .join(datesDF,
        (datesDF("CodComptes") === dataDF("CodCompte")) && (dataDF("DateExtrait") === datesDF("DateExtraitS"))
        , "inner")
      .distinct()
        .drop("CodComptes","DateExtraitS")



    val datasetDF = soldesDF.join(MasterCompteBKDF(sqlContext),"CodCompte")

            datasetDF
              .repartition(1)
              .write
              .format("com.databricks.spark.csv")
              .option("header", "true")
              .option("delimiter", ";")
              .save("src\\TargetData\\BkCompteData")


  }

  def MasterCompteBKDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\BKCompteData
    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
      .select(
        $"GCO_CodCompte".as("CodCompte"),
        $"GCO_IsOuvert".as("IsOuvert"),
        $"GCO_IsAsv".as("IsAsv"),
        $"GCO_IsAssureur".as("IsAssureur"),
        $"GCO_IsPea".as("IsPea"),
        $"GCO_IsPeaPme".as("IsPeaPme"),
        $"GCO_CodProduit".as("CodProduit"),
        $"GCO_TypGestion".as("TypGestion"),
        $"GCO_LibEtatCompte".as("LibEtatCompte"),
        $"GCO_CodCapaciteSupportPertesMif2".as("CodCapaciteSupportPertesMif"),
        $"GCO_CodProfilInvestissementMif2".as("CodProfilInvestissementMif"),
        $"GCO_CodToleranceRisqueMif2".as("CodToleranceRisqueMif"),
        $"GCO_CodHorizonPlacementMif2".as("CodHorizonPlacementMif"),
        $"GCO_CodObjectifPlacementMif2".as("CodObjectifPlacementMif"),
        $"GCO_CodTypeDistributionMif2".as("CodTypeDistributionMif"),
        $"GCO_CodProfilRisqueMif2".as("CodProfilRisqueMif"),
        $"GCO_CodStrategieInvestissementMif2".as("CodStrategieInvestissementMif"),
        $"GCO_IsNanti".as("IsNanti")
      )
      .filter($"IsPea" === 0 && $"IsPea".isNotNull && $"IsPeaPme" === 0 && $"IsPeaPme".isNotNull)
      .drop("IsPea", "IsPeaPme")
  }

  def RefProduitDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\RefProduit
    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
      .select(
        $"GCO_CodProduit".as("CodProduit"),
        $"GCO_LibProduit".as("LibProduit")
      )
      .na.drop()
      .distinct()
  }

  def RefTypGestionDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\RefTypGestion
    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\REF_EW84_CodTypeMandatGestion.csv")
      .select(
        $"EW84_PKCode".as("CodTypGestion"),
        $"EW84_Libelle".as("LibTypGestion")
      )
      .na.drop()
  }

  def RefEtatCompteDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
      .select(
        //   $"EW84_PKCode".as("CodTypGestion"),
        $"GCO_LibEtatCompte".as("LibEtatCompte")
      )
      .withColumn("CodEtatCompte", monotonicallyIncreasingId)
      .na.drop()
      .distinct()
  }

  def RefDataDF(sqlContext: SQLContext): DataFrame = {

    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\REF_RML_ReferentielsMultiLangues.csv")
      .select("RML_PKCode", "RML_Libelle1")

  }

  def SoldesDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\RefTypGestion
    val dataDF =

      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\HIS_POH_PositionHisto__extrait.csv")
        .select(
          $"POH_CodCompte".as("CodCompte"),
          $"POH_DatArrete".as("DateExtrait"),
          $"POH_MntValorisationTitresEnEuros".as("SoldeTitres"),
          $"POH_QteTitres".as("SoldeEnEuro")
        )
        .unionAll(
          sqlContext.read.format("csv")
            .option("header", "true")
            .option("delimiter", ";")
            .option("inferSchema", "true")
            .load("src\\SourceData\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")
            .select(
              $"PGH_CodCompte".as("CodCompte"),
              $"PGH_DatArrete".as("DateExtrait"),
              $"PGH_MntTotalValorisationTitresEnEuros".as("SoldeTitres"),
              $"PGH_MntSoldeEuroEnEuros".as("SoldeEnEuro")
            )
        )

    val datesDF = dataDF.select("CodCompte", "DateExtrait")
      .groupBy("CodCompte").agg(max("DateExtrait") as "DateExtraitS")
      .distinct()
      .withColumnRenamed("CodCompte", "CodComptes")


    val soldesDF = dataDF
      .join(datesDF,
        (datesDF("CodComptes") === dataDF("CodCompte")) && (dataDF("DateExtrait") === datesDF("DateExtraitS"))
        , "inner")
      .distinct()
      .drop("CodComptes","DateExtraitS")



    return  soldesDF.join(MasterCompteBKDF(sqlContext),"CodCompte")
  }


}


