package TestingPackage

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col, count, regexp_replace}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._


object ChurnDetection {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    // conf.set("es.index.auto.create", "true")
    //  conf.set("es.nodes", "127.0.0.1")
    //    conf.set("es.port","9200")


    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("ERROR")
    /*
        val bqClientDF = sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("src\\data\\CLI_GTI_GeneriquesTiers.csv")

          //Suppression des autres colonnes
          .drop("GTI_CodSociete")
          .drop("GTI_CodPrestataire")
          .drop("EW28_Libelle")
          .drop("GTI_NumStatutPers")
          .drop("GTI_CodTypeInvestisseurMif2")
          .drop("GTI_CodExperienceMif2")
          .drop("GTI_CodCapaciteProduitComplexeMif2")

          //Changement des noms de colonnes
          .withColumnRenamed("GTI_CodTiers", "IdClient")
          .withColumnRenamed("civilite", "Civilite")
          .withColumnRenamed("GTI_DatNaissanceOuCreation", "DatNaissanceOuCreation")
          .withColumnRenamed("csp", "GrpProfession")
          .withColumnRenamed("DepartementResidence", "Agence")
          .withColumn("DatNaissanceOuCreation", $"DatNaissanceOuCreation" cast "Int")

        bqClientDF.cache()


        val crmContactDF = sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("src\\data\\CRM_V_CONTACTS_v2.csv")


          //Changement des noms de colonnes
          .withColumnRenamed("Id", "IdContact")
          .withColumnRenamed("code2", "IdClient")
          .withColumnRenamed("Status", "TypeContact")
          .withColumn("ClientPhoneContactFrequency", regexp_replace(col("ClientPhoneContactFrequency"), "NULL", "0"))
          .withColumn("ClientMeetingFrequency", regexp_replace(col("ClientMeetingFrequency"), "NULL", "0"))
          .withColumn("MeetingFrequency", regexp_replace(col("MeetingFrequency"), "NULL", "0"))
          .withColumn("ClientPhoneContactFrequency",$"ClientPhoneContactFrequency" cast "Int")
          .withColumn("ClientMeetingFrequency",$"ClientMeetingFrequency" cast "Int")
          .withColumn("MeetingFrequency",$"MeetingFrequency" cast "Int")

        // jointure des deux dataframe client et contact en un seul customer record

          crmContactDF.cache()
        // chargement du fichier des interaction

        val crmInteractionDF= sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter",";")
          .option("inferSchema", "true")
          .load("src\\data\\CRM_V_INTERACTIONSOBP.csv")

          //Changement des noms de colonnes
          .withColumnRenamed("contactId","IdContact")
         crmInteractionDF.cache()

        val typeActDF = crmInteractionDF.select("ActivityType").distinct().collect().map(_(0)).toList


        val sumaryInteractionDF = crmInteractionDF.select("IdContact","ActivityType")
          .groupBy("IdContact","ActivityType")
          .agg(count("IdContact").as("count"))
          .groupBy("IdContact")
          .pivot("ActivityType", typeActDF)
          .sum("count")
          .na.fill(0)
        // .withColumnRenamed("IdContact","IdContact")
          sumaryInteractionDF.cache()
        val lienDF= sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter",";")
          .option("inferSchema", "true")
          .load("src\\data\\CLI_TCL_TiersComptesLocal.csv")

          //Suppression des autres colonnes
          .drop("TCL_CodSociete")
          .drop("TCL_NumLien")

          //Changement des noms de colonnes
          .withColumnRenamed("TCL_CodTiers","IdClient")
          .withColumnRenamed("TCL_CodCompte_hash","IdCompte")
          .withColumnRenamed("CRT_Libelle","TypeLien")

       lienDF.cache()

        val customerDF = crmContactDF
          .join(bqClientDF,"IdClient")
          .join(sumaryInteractionDF,"IdContact")

          customerDF.cache()
            // chargement du fichier des comptes

            val compteDF= sqlContext.read.format("csv")
              .option("header", "true")
              .option("delimiter",";")
              .option("inferSchema", "true")
              .load("src\\data\\CLI_GCO_GeneriquesComptes.csv")

              //Suppression des autres colonnes
              .drop("GCO_CodSociete")
              .drop("GCO_CodPrestataire")
              .drop("GCO_CodTypeOrientation")
              .drop("GCO_TypGestion")
              .drop("GCO_CodTypContrat")
              .drop("GCO_CodTypeDistributionMif2")
              .drop("GCO_CodTarif")
              .drop("GCO_CodTarifDDG")
              .drop("GCO_CodTarifFHG")


              //Changement des noms de colonnes
              .withColumnRenamed("GCO_CodCompte","IdCompte")
              .withColumnRenamed("GCO_IsOuvert","IsOuvert")
              .withColumnRenamed("GCO_IsAsv","IsAsv")
              .withColumnRenamed("GCO_IsAssureur","IsAssureur")
              .withColumnRenamed("GCO_IsPea","IsPea")
              .withColumnRenamed("GCO_IsPeaPme","IsPeaPme")
              .withColumnRenamed("GCO_IsPeaPme","IsPeaPme")
              .withColumnRenamed("GCO_CodProduit","IdProduit")
              .withColumnRenamed("GCO_LibProduit","LibProduit")
              .withColumnRenamed("GCO_LibEtatCompte","LibEtatCompte")
              .withColumnRenamed("GCO_CodEnveloppeFiscale","CodEnveloppeFiscale")
              .withColumnRenamed("GCO_CodTribu","CodContactCrm")
              .withColumnRenamed("GCO_CodCapaciteSupportPertesMif2","CodCapaciteSupportPertes")
              .withColumnRenamed("GCO_CodProfilInvestissementMif2","CodProfilInvestissement")
              .withColumnRenamed("GCO_CodToleranceRisqueMif2","CodToleranceRisque")
              .withColumnRenamed("GCO_CodHorizonPlacementMif2","CodHorizonPlacement")
              .withColumnRenamed("GCO_CodObjectifPlacementMif2","CodObjectifPlacement")
              .withColumnRenamed("GCO_CodProfilRisqueMif2","CodProfilRisque")
              .withColumnRenamed("GCO_CodStrategieInvestissementMif2","CodStrategieInvestissement")
              .withColumnRenamed("GCO_AllocationActionMinMif2","AllocationActionMin")
              .withColumnRenamed("GCO_AllocationActionMaxMif2","AllocationActionMax")
              .withColumnRenamed("GCO_IsNanti","IsNanti")*/

    // println(compteDF.filter(compteDF("IsOuvert")=!=0 && compteDF("LibEtatCompte")==="Ordinaire").count())
    // compteDF.filter(compteDF("IsOuvert")=!=0 && compteDF("LibEtatCompte")==="Ordinaire").select("LibProduit").distinct().show(50)
    /*    compteDF.cache()
        val fullCompteDF = lienDF.join(compteDF.filter(compteDF("LibEtatCompte")==="Ordinaire"),"IdCompte")
        fullCompteDF.cache()
        val fullCustomerDF = customerDF.join(fullCompteDF,"IdClient")
        fullCustomerDF.cache()
        fullCustomerDF.printSchema()
        fullCustomerDF.show(5)
        println(fullCustomerDF.select("IdClient","IdContact").distinct().count())
        fullCustomerDF.select("IdClient","IdContact").distinct()
          .repartition(1)
          .write.format("com.databricks.spark.csv")
          .option("header", "true")
          .save("fullcustomersInfo.csv")*/

    val lienDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")

      //Changement des noms de colonnes
      .select(
      $"TCL_CodTiers".as("IdClient"),
      $"TCL_CodCompte_hash".as("IdCompte"),
      $"CRT_Libelle".as("TypeLien"))

    val compteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\Comptes\\data.csv")
      .select("IdCompte", "IsOuvert")
      .na.fill(0)

    val clientChurnDF =
      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .load("src\\GeneratedData\\fullcustomersInfo\\data.csv")
        .join(lienDF, "IdClient")
        .join(compteDF, "IdCompte")
        .select("IdClient", "IsOuvert")
        .groupBy("IdClient")
        .agg(sum("IsOuvert").as("Churn"))
        .withColumn("Churn", when($"Churn" === 0, 1).otherwise(0))

    println(clientChurnDF.count())
    clientChurnDF.groupBy("Churn")
      .agg(count("Churn").as("Count"))
      .show(5)


    clientChurnDF
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\GeneratedData\\CustomerChurn")

  }
}