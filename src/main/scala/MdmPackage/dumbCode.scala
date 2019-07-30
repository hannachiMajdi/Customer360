package MdmPackage

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object dumbCode {


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
        .load("src\\SourceData\\MEN_GCO_GeneriquesComptes.csv")
    val df =  sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")

   val joinDF =  dataDF.join(df,df("TCL_CodCompte_hash")===dataDF("GCO_CodCompte"),"outer"
      )
    joinDF
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\TargetData\\FaitComptes2")

/*
    val dataDF =
      sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
        .select(

        $"Id".as("CodContact"),
        $"code2".as("CodTiers"),
        $"Status",
        $"ClientPhoneContactFrequency",
        $"ClientMeetingFrequency",
        $"MeetingFrequency",
        $"PhoneContactFrequency"
      )
        .join(DemographicDF(sqlContext),"CodTiers")

    dataDF.printSchema()
    dataDF.describe().show()*/

 /*   dataDF
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\TargetData\\BKCustomerData")
*/

  }

  def BKCustomerDataDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\RefProduit
    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
      .select(

        $"Id".as("CodContact"),
        $"code2".as("CodTiers"),
        $"Status",
        $"ClientPhoneContactFrequency",
        $"ClientMeetingFrequency",
        $"MeetingFrequency",
        $"PhoneContactFrequency"
      )
      .join(DemographicDF(sqlContext),"CodTiers")
  }
  def DemographicDF(sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    //src\TargetData\RefProduit
    val TiersDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\names\\names.csv")
      .join(sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv"), "GTI_CodTiers")

    val lienDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")

    val compteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\TargetData\\BkCompteData\\data.csv")
      .withColumnRenamed("CodCompte", "TCL_CodCompte_hash")


    return lienDF.join(TiersDF, lienDF("TCL_CodTiers") === TiersDF("GTI_CodTiers"))
      .join(compteDF, "TCL_CodCompte_hash")
      .select(
        $"GTI_CodTiers".as("CodTiers"),
        $"profession",
        $"nomComplet",
        $"Sexe",
        $"SituationFamiliale",
        $"PaysNaissance",
        $"civilite",
        $"csp".as("GroupeSociale"),
        $"GTI_CodTypeInvestisseurMif2".as("CodTypeInvestisseurMif2"),
        $"GTI_CodExperienceMif2".as("CodExperienceMif2"),
        $"GTI_CodCapaciteProduitComplexeMif2".as("CodCapaciteProduitComplexeMif2"),
        $"DepartementResidence"
      ).distinct()
  }


}


