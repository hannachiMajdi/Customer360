package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
//import org.elasticsearch.spark.sql._

object ReadCsv {

  /*case class Client(IdClient:String,PaysNaissance:String,Civilite:String,DatNaissanceOuCreation:Int,Sexe:String,SituationFamiliale:String,GrpProfession:String
  ,Profession:String,Agence:String )
  case class Lien(IdCompte:String ,IdClient:String , TypeLien:String)*/

  def main(args : Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "127.0.0.1")
    conf.set("es.port","9200")


    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //----------Données du banque--------------------------
      //Chargement de la liste des clients

           val clientDF= sqlContext.read.format("csv")
             .option("header", "true")
             .option("delimiter",";")
             .option("inferSchema", "true")
             .load("src\\data\\CLI_GTI_GeneriquesTiers.csv")

         //Suppression des autres colonnes
               .drop("GTI_CodSociete")
               .drop("GTI_CodPrestataire")
               .drop("EW28_Libelle")
               .drop("GTI_NumStatutPers")
               .drop("GTI_NumStatutPers")
               .drop("GTI_CodTypeInvestisseurMif2")
               .drop("GTI_CodExperienceMif2")
               .drop("GTI_CodCapaciteProduitComplexeMif2")

         //Changement des noms de colonnes
               .withColumnRenamed("GTI_CodTiers","IdClient")
               .withColumnRenamed("civilite","Civilite")
               .withColumnRenamed("GTI_DatNaissanceOuCreation","DatNaissanceOuCreation")
               .withColumnRenamed("csp","GrpProfession")
               .withColumnRenamed("DepartementResidence","Agence")

         val clientRDD = clientDF.rdd

      val testDF = clientDF.select("Civilite").distinct()
    testDF.printSchema()
    //testDF.saveToEs("test/HelloES")
    // chargement du fichier relation entre tiers et comptes
    /*
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
  */
        // chargement du fichier des comptes
    /*
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
          .withColumnRenamed("GCO_IsNanti","IsNanti")
    */
        // chargement du fichier de l'historique du compte
    /*
        val histoCompteDF= sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter",";")
          .option("inferSchema", "true")
          .load("src\\data\\MEN_GCO_GeneriquesComptes.csv")

          //Suppression des autres colonnes
          .drop("GCO_CodSociete")
          .drop("GCO_CodPrestataire")
          .drop("GCO_CodTypeOrientation")
          .drop("GCO_CodTarif")
          .drop("GCO_CodTarifDDG")
          .drop("GCO_CodTarifFHG")


          //Changement des noms de colonnes
          .withColumnRenamed("GCO_AnMois","AnMois")
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
          .withColumnRenamed("GCO_IsNanti","IsNanti")

         */
    // chargement du fichier des transaction
    /*
    val transactionDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\CRO_CRO_CROD.csv")

         //Suppression des autres colonnes
         .drop("CRO_CodSociete")
         .drop("CRO_Crs")
         .drop("CRO_MntBrutDevDep")

         //Changement des noms de colonnes
         .withColumnRenamed("CRO_CodCompte","IdCompte")
         .withColumnRenamed("CRO_Dateffet","Dateffet")
         .withColumnRenamed("CRO_CodOperation","CodOperation")
         .withColumnRenamed("CRO_LibOperation","LibOperation")
         .withColumnRenamed("CRO_CodAnnulation","CodAnnulation")
         .withColumnRenamed("CRO_CodSens","CodSens")
         .withColumnRenamed("CRO_Qte","Montant")
         .withColumnRenamed("CRO_CodIsin","CodIsin")
         .withColumnRenamed("ESO_CodType","CodType")
         .withColumnRenamed("ESO_CodProvenance","CodProvenance")
*/
    // chargement du fichier des instruments de transaction
    /*
    val instrumentDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\INS_GIN_GeneriqueInstruments.csv")

      //Suppression des autres colonnes
      .drop("GIN_Actif")
      .drop("GIN_CodPaysEmission")
      .drop("GIN_CodDeviseCotation")
      .drop("GIN_CodMIC")
      //Changement des noms de colonnes
      .withColumnRenamed("GIN_CodISIN","CodIsin")
      .withColumnRenamed("KW02_Libelle","Lib")
      .withColumnRenamed("GIN_CodProduitComplexe","ProduitComplexe")
      .withColumnRenamed("GIN_LibEmetteur","LibEmetteur")
      .withColumnRenamed("GIN_LibInstrument","LibInstrument")
*/

    // chargement du fichiers des extraits bancaires des comptes
    /*
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
      .withColumnRenamed("POH_CodIsin","CodIsin")
      .withColumnRenamed("POH_MntValorisationTitresEnEuros","SoldeEnEuro")
  */
    // chargement du fichier de 'historique de changement de solde
    /*
    val histoExtraitDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")

      //Suppression des autres colonnes
      .drop("PGH_CodSociete")



      //Changement des noms de colonnes
      .withColumnRenamed("PGH_CodCompte","IdCompte")
      .withColumnRenamed("PGH_DatArrete","DateExtrait")
      .withColumnRenamed("PGH_MntTotalValorisationTitresEnEuros","SoldeTitres")
      .withColumnRenamed("PGH_MntSoldeEuroEnEuros","SoldeEnEuro")
*/

  //-----------------Fin Données bancaires -----------------
  //----------------- Données du CRM -----------------------
    // chargement du fichier Contact (client)
    /*
          val crmContactDF= sqlContext.read.format("csv")
              .option("header", "true")
              .option("delimiter",";")
              .option("inferSchema", "true")
              .load("src\\data\\CRM_V_CONTACTS.csv")


            //Changement des noms de colonnes
            .withColumnRenamed("Id","IdContact")
            .withColumnRenamed("code","IdClient")
            .withColumnRenamed("Status","TypeContact")

    */
    // chargement des fichier des Account (entreprise)
    /*
    val crmCompteDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\CRM_V_ACCOUNTS.csv")

      //Suppression des autres colonnes
      .drop("RMSecFixedIncomeID")
      .drop("RMSecEquitiesID")

      //Changement des noms de colonnes
      .withColumnRenamed("Id","IdAccount")
      .withColumnRenamed("code","IdClient")
      .withColumnRenamed("BECode","IdContact")
*/
    // chargement du fichier des interaction
    /*
    val crmInteractionDF= sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter",";")
      .option("inferSchema", "true")
      .load("src\\data\\CRM_V_INTERACTIONSOBP.csv")

      //Changement des noms de colonnes
      .withColumnRenamed("contactId","IdContact")
    */
    // chargement des relation entre contacts
    /*
  val crmLienDF= sqlContext.read.format("csv")
    .option("header", "true")
    .option("delimiter",";")
    .option("inferSchema", "true")
    .load("src\\data\\ContactRelation_OBP_CRM.csv")

  */
  //--------------- Fin Données du CRM ---------------------------
    //crmLienDF.printSchema()

  }
}