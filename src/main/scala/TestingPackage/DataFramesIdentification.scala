package TestingPackage

//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
//import org.elasticsearch.spark.sql._


object DataFramesIdentification {



  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

  //  conf.set("es.index.auto.create", "true")
    //conf.set("es.nodes", "127.0.0.1")
   // conf.set("es.port","9200")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("ERROR")


    //-----------------Creating customer entity---------------------------------

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




    val crmContactDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")


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


    // chargement du fichier des interaction

    val crmInteractionDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\CRM_V_INTERACTIONSOBP.csv")

      //Changement des noms de colonnes
      .withColumnRenamed("contactId","IdContact")
   // crmInteractionDF.cache()

    val typeActDF = crmInteractionDF.select("ActivityType").distinct().collect().map(_(0)).toList


    val sumaryInteractionDF = crmInteractionDF.select("IdContact","ActivityType")
          .groupBy("IdContact","ActivityType")
          .agg(count("IdContact").as("count"))
          .groupBy("IdContact")
          .pivot("ActivityType", typeActDF)
          .sum("count")
          .na.fill(0)
         // .withColumnRenamed("IdContact","IdContact")

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

    println(lienDF.select("IdClient").distinct().count())
/*


    val customerDF = crmContactDF
      .join(bqClientDF, bqClientDF("IdClientBQ") === crmContactDF("IdClient") || (bqClientDF("IdClientBQ").isNull &&  crmContactDF("IdClient").isNull) ,"outer")
      .join(sumaryInteractionDF, sumaryInteractionDF("IdContactS") === crmContactDF("IdContact") || (sumaryInteractionDF("IdContactS").isNull &&  crmContactDF("IdContact").isNull) ,"outer")
      .join(bqClientDF, bqClientDF("IdClientBQ") === crmContactDF("IdClient") || (bqClientDF("IdClientBQ").isNull &&  crmContactDF("IdClient").isNull) ,"outer")
      .distinct()
      .drop("IdContactS")
      .drop("IdClientBQ")*/

      val customerDF = crmContactDF
          .join(bqClientDF,"IdClient")
          .join(sumaryInteractionDF,"IdContact")
          .join(lienDF,"IdClient")
      .distinct()

   println(customerDF.select("IdClient").distinct().count())
    customerDF.printSchema()
    customerDF.show(10)










    //crmInteractionDF.printSchema()
    //-----------------------------Fin Customer Entity------------------------------------
/*
    //----------------------------- Create Account entity---------------------------------
    // chargement du fichier relation entre tiers et comptes

          val compteClientDF= sqlContext.read.format("csv")
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
            .filter($"IdClient".isNotNull && $"IdCompte".isNotNull)


    // chargement du fichier des comptes

        val compteAlDF= sqlContext.read.format("csv")
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
          .withColumnRenamed("GCO_IsNanti","IsNanti")
          .filter($"IdCompte".isNotNull)

    val produitDF = compteAlDF.select("IdProduit","LibProduit").distinct()
    val compteDF = compteAlDF.drop("LibProduit")

    //------------------------- Extrait du soldes ---------------------------
    val extraitDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\HIS_POH_PositionHisto__extrait.csv")

      //Suppression des autres colonnes
      .drop("POH_CodSociete")
      .drop("POH_CodPrestataire")


      //Changement des noms de colonnes
      .withColumnRenamed("POH_CodCompte","IdCompte")
      .withColumnRenamed("POH_DatArrete","DateExtrait")
      .withColumnRenamed("POH_QteTitres","SoldeTitres")
    //  .withColumnRenamed("POH_CodIsin","CodIsin")
      .withColumnRenamed("POH_MntValorisationTitresEnEuros","SoldeEnEuro")

      val compteCompletDF = compteDF.join(extraitDF,"IdCompte")

    compteCompletDF.printSchema()

*/

  }

}


