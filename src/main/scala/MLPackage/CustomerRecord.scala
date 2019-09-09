package MLPackage

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._

import org.apache.spark.SparkContext._

object CustomerRecord {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SingleRecord")
      .setMaster("local[*]")
      .set("es.index.auto.create", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.es.net.ssl","true")
      .set("spark.es.nodes",  "127.0.0.1")
      .set("spark.es.port", "9200")
    //.set("spark.es.net.http.auth.user","elastic")
    //.set("spark.es.net.http.auth.pass", "jmYf8ihvwQBMbF9S7HRdfouf")
    //.set("spark.es.resource", indexName)
    // .set("spark.es.nodes.wan.only", "true")

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
      //.load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")
      .load("hdfs://localhost:9000/DataLake/CoreBanking/Comptes/*.csv")

    val BridgeDF = sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_bridge_client_compte")
      /*sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-f7a024e9-7641-452b-afd2-57e21469d1b0-c000.csv")

       */

    val faitSoldeDF = sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_fait_soldeactivity")
    /*sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_soldeactivity\\part-00000-2c90d107-75dc-4d66-a6fb-23d88212d261-c000.csv")

     */

    val faitCompteDF = sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_fait_compteactivity")
      /*sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_compteactivity\\part-00000-60cdb97e-89ce-4bd3-b2cc-1c18a7cd9380-c000.csv")

       */

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
    val PersonalInfoDF = sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_dimension_client")
      /*sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_dimension_client\\part-00000-dd5db9fc-38d2-4899-854a-a9819a0eb7e4-c000.csv")

       */

    // ________________Nbr Nantissement _____________________

    val ClientExperienceDF = BridgeDF
      .join(faitCompteDF, "FK_CodCompte")
      .groupBy("FK_CodTiers")
      .agg(
        sum("IsNanti").as("NbrNantissement")
      )
    // ________________Attribution de crédit , segments _____________________ GCO_CodCapaciteSupportPertesMif2,GCO_CodStrategieInvestissementMif2

    val ClientMLLabesDF =
      sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
     // .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv")
        .load("hdfs://localhost:9000/DataLake/CoreBanking/Tiers/*.csv")
       .select($"GTI_CodTiers".as("FK_CodTiers"),
         $"GTI_CodCapaciteProduitComplexeMif2"
       )
    .withColumn("AttributionCredit",
      when($"GTI_CodCapaciteProduitComplexeMif2"==="01",0)
        .otherwise(when($"GTI_CodCapaciteProduitComplexeMif2"==="02",1).otherwise(null))

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

    // ________________Nbr Transaction_____________________

    val nbrTransactionDF = sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_fait_transaction")
      /*sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      //.load("src\\DW\\dw_fait_transaction\\part-00000-f39a60be-c38c-41e2-b585-81b097db8988-c000.csv")

       */

      .groupBy("FK_CodCompte", "Qte")
      .agg(count("Qte").as("nbrOperation"))
      .join(BridgeDF, "FK_CodCompte")
      .groupBy("FK_CodTiers")
      .agg(sum("nbrOperation").as("nbrTransaction"))
      .na.fill(0)

    // ________________Nbr Reclamation_____________________

    val crmInteractionDF =/* sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_interaction\\part-00000-d1f3496e-b823-46d6-be15-fec204ead6dd-c000.csv")
      */
      sqlContext.read.format("org.elasticsearch.spark.sql").load("dw_fait_interaction")


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
          $"nbrTransactionMensuel".isNotNull
         // $"Churn".isNotNull
      )

      .withColumn("Churn",
        when(
          $"quitter" === 0 &&
          $"soldeTitre" <100 && $"soldeLiquide" <100 && $"soldeTitre"> -100 && $"soldeLiquide" > -100
          , 1)
          .otherwise(when(
            $"quitter" =!= 0 &&
              $"soldeTitre" <100 && $"soldeLiquide" <100 && $"soldeTitre"> -100 && $"soldeLiquide" > -100

            , 0)
            .otherwise(null)))
      .drop(
        "PaysDeNaissance",
        "Politesse",
        "Genre",
        "SituationFamilial",
        "GroupeProfession",
        "Fonction",
        "DepartementResidence",
        "GTI_CodCapaciteProduitComplexeMif2",
        "quitter")


    DataDF

      .saveToEs("ml_customer_record")
     /* .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\InputRecord_2")*/




  }
}


