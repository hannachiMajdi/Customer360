package MLPackage

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object CustomerRecord {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SingleRecord")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._


    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)

    val CompteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")

    val BridgeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-f7a024e9-7641-452b-afd2-57e21469d1b0-c000.csv")

    val faitSoldeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_soldeactivity\\part-00000-2c90d107-75dc-4d66-a6fb-23d88212d261-c000.csv")

    val faitCompteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_compteactivity\\part-00000-60cdb97e-89ce-4bd3-b2cc-1c18a7cd9380-c000.csv")

    val MaxDateFaitDF = faitSoldeDF
      .select(
        $"FK_CodCompte".as("CodCompte"),
        $"FK_Date"
      )
      .groupBy("CodCompte")
      .agg(
        max("FK_Date").as("MaxDate")
      )

    val MinDateFaitDF = faitCompteDF
      .select(
        $"FK_CodCompte".as("CodCompte"),
        $"FK_Date"
      )
      .groupBy("CodCompte")
      .agg(
        min("FK_Date").as("MinDate")
      )
    val PersonalInfoDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_dimension_client\\part-00000-cf84fc22-ae24-472b-b5bf-ac567852483b-c000.csv")

    // ________________Nbr Nantissement _____________________

    val ClientExperienceDF = BridgeDF
      .join(faitCompteDF, "FK_CodCompte")
      .groupBy("FK_CodTiers")
      .agg(
        sum("IsNanti").as("NbrNantissement")
      )
    // ________________Attribution de crédit , segments _____________________ GCO_CodCapaciteSupportPertesMif2,GCO_CodStrategieInvestissementMif2

    val ClientMLLabesDF = BridgeDF
      .join(CompteDF
        .withColumn("CapaciteSupportPertes", when($"GCO_CodCapaciteSupportPertesMif2".cast("Int") === 1, 1).otherwise(0))
        // .withColumn("CodStrategieInvestissementMif",when($"GCO_CodStrategieInvestissementMif2".cast("Int")===1,1).otherwise(0))

        , BridgeDF("FK_CodCompte") === CompteDF("GCO_CodCompte"))
      .groupBy("FK_CodTiers")
      .agg(
        first("CapaciteSupportPertes").as("AttributionCredit")
        //first("CodStrategieInvestissementMif").as("Segment")
      )
    // ________________Info Client + Experience + nombre de produit_____________________

    val ClientCompteInfoDF = BridgeDF
      .join(
        faitCompteDF
          .join(MinDateFaitDF,
            MinDateFaitDF("CodCompte") === faitCompteDF("FK_CodCompte")
              &&
              MinDateFaitDF("MinDate") === faitCompteDF("FK_Date")
          )
          .select(
            $"FK_CodCompte",
            substring($"FK_Date".cast("String"), 1, 4).as("DateAjout")
          )
          .withColumn("ExperienceEnAnnee", abs($"DateAjout" - Year))
          .drop("DateAjout").na.fill(0)
        , "FK_CodCompte"
      )
      .groupBy("FK_CodTiers")
      .agg(
        max("ExperienceEnAnnee").as("ExperienceEnBQ"),
        count("FK_CodCompte").as("nbProduit"))



    // ________________Attribut Churn + Solde _____________________

    val ChurnDF = CompteDF
      .select(
        $"GCO_CodCompte".as("FK_CodCompte"),
        $"GCO_IsOuvert"
      )
      .join(
        BridgeDF
          .select(
            $"FK_CodCompte",
            $"FK_CodTiers"
          )
        , "FK_CodCompte"
      )
      .join(
        faitSoldeDF
          .join(MaxDateFaitDF,
            MaxDateFaitDF("CodCompte") === faitSoldeDF("FK_CodCompte")
              &&
              MaxDateFaitDF("MaxDate") === faitSoldeDF("FK_Date")
          )
          .select(
            $"FK_CodCompte",
            $"MntTotalValorisationTitresEnEuros",
            $"MntSoldeEuroEnEuros"
          )
        , "FK_CodCompte"
      )
      .groupBy("FK_CodTiers")
      .agg(
        //   max("ExperienceEnAnnee").as("ExperienceEnBQ"),
        sum("GCO_IsOuvert").as("quitter"),
        sum("MntTotalValorisationTitresEnEuros").as("soldeTitre"),
        sum("MntSoldeEuroEnEuros").as("soldeLiquide")
      )
      .withColumn("Churn", when($"quitter" === 0, 1).otherwise(0))

    // ________________Nbr Transaction_____________________

    val nbrTransactionDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_transaction\\part-00000-78951a1c-3a61-42e1-9bbd-7a4d3d6a550b-c000.csv")
      .groupBy("FK_CodCompte", "Qte")
      .agg(count("Qte").as("nbrOperation"))
      .join(BridgeDF, "FK_CodCompte")
      .groupBy("FK_CodTiers")
      .agg(sum("nbrOperation").as("nbrTransaction"))
      .na.fill(0)

    // ________________Nbr Reclamation_____________________

    val crmInteractionDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_interaction\\part-00000-d1f3496e-b823-46d6-be15-fec204ead6dd-c000.csv")


    val pivotDF = crmInteractionDF
      .groupBy("FK_CodTiers")
      .agg(count("FK_CodTiers").as("NbrReclamation"))
      .withColumnRenamed("FK_CodTiers", "CodTiers")
      .na.fill(0)


    val DataDF = ChurnDF
      .withColumnRenamed("FK_CodTiers", "CodTiers")
      .join(PersonalInfoDF, "CodTiers")
      .join(pivotDF, "CodTiers")
      .join(nbrTransactionDF.withColumnRenamed("FK_CodTiers", "CodTiers"), "CodTiers")
      .join(ClientCompteInfoDF.withColumnRenamed("FK_CodTiers", "CodTiers"), "CodTiers")
      .join(ClientExperienceDF.withColumnRenamed("FK_CodTiers", "CodTiers"), "CodTiers")
      .join(ClientMLLabesDF.withColumnRenamed("FK_CodTiers", "CodTiers"), "CodTiers")


      .withColumn("nbrTransactionMensuel", round($"nbrTransaction" / ($"ExperienceEnBQ" * 12)))
      .withColumn("PaysNaissance", when($"PaysDeNaissance".isNull or $"PaysDeNaissance" === "NULL", "france").otherwise($"PaysDeNaissance"))
      .withColumn("Civilite", when($"Politesse".isNull or $"Politesse" === "NULL", "autre").otherwise($"Politesse"))
      .withColumn("Sexe", when($"Genre".isNull or $"Genre" === "NULL", "autre").otherwise($"Genre"))
      .withColumn("SituationFamiliale", when($"SituationFamilial".isNull or $"SituationFamilial" === "NULL", "autre").otherwise($"SituationFamilial"))
      .withColumn("GroupProfession", when($"GroupeProfession".isNull or $"GroupeProfession" === "NULL", "autre").otherwise($"GroupeProfession"))
      .withColumn("Profession", when($"Fonction".isNull or $"Fonction" === "NULL", "autre").otherwise($"Fonction"))
      .withColumn("Departement", when($"DepartementResidence".isNull or $"DepartementResidence" === "NULL", "autre").otherwise($"DepartementResidence"))

      .filter(
        $"CodTiers".isNotNull && $"CodTiers" =!= "NULL" &&
          $"soldeTitre".isNotNull &&
          $"soldeLiquide".isNotNull &&
          $"Status".isNotNull && $"Status" =!= "NULL" &&
          $"NomComplet".isNotNull && $"NomComplet" =!= "NULL" &&
          $"SituationFamiliale".isNotNull && $"SituationFamiliale" =!= "NULL" &&
          $"GroupProfession".isNotNull && $"GroupProfession" =!= "NULL" &&
          $"Profession".isNotNull && $"Profession" =!= "NULL" &&
          $"DepartementResidence".isNotNull &&
          $"Age".isNotNull &&
          $"nbrTransaction".isNotNull &&
          $"NbrReclamation".isNotNull &&
          $"ExperienceEnBQ".isNotNull &&
          $"nbProduit".isNotNull &&
          $"nbrTransactionMensuel".isNotNull &&
          $"Churn".isNotNull
      )
      .drop(
        "PaysDeNaissance",
        "Politesse",
        "Genre",
        "SituationFamilial",
        "GroupeProfession",
        "Fonction",
        "DepartementResidence",
        "quitter")
      .na.drop()

    DataDF

      //.saveToEs("dw_dimension_client/client")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\InputRecord")


  }
}


