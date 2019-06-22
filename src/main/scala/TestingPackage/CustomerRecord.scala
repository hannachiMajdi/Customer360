package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object CustomerRecord {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val bqClientDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv")

      .select($"GTI_CodTiers".as("IdClient")
        , $"civilite".as("Civilite")
        , $"GTI_DatNaissanceOuCreation".as("DatNaissanceOuCreation").cast("Int")
        , $"csp".as("GrpProfession")
        , $"DepartementResidence".as("Agence")
        , $"SituationFamiliale"
        , $"profession"
        , $"Sexe"
        , $"PaysNaissance"
      )


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
      .withColumn("ClientPhoneContactFrequency", $"ClientPhoneContactFrequency" cast "Int")
      .withColumn("ClientMeetingFrequency", $"ClientMeetingFrequency" cast "Int")
      .withColumn("MeetingFrequency", $"MeetingFrequency" cast "Int")

    // chargement du fichier des interaction
    /*
        val crmInteractionDF = sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("src\\SourceData\\CRM_V_INTERACTIONSOBP.csv")

          //Changement des noms de colonnes
          .withColumnRenamed("contactId", "IdContact")
        // crmInteractionDF.cache()

              val typeActDF = crmInteractionDF.select("ActivityType").distinct().collect().map(_(0)).toList


              val sumaryInteractionDF = crmInteractionDF.select("IdContact","ActivityType")
                .groupBy("IdContact","ActivityType")
                .agg(count("IdContact").as("count"))
                .groupBy("IdContact")
                .pivot("ActivityType", typeActDF)
                .sum("count")
                .na.fill(0)
        */
    val customerDF = crmContactDF
      .join(bqClientDF, "IdClient")
      //     .join(sumaryInteractionDF,"IdContact")
      .join(sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\InteractionSummary\\data.csv")
      , "IdContact")
      .na.fill(0)
      .withColumn("NbrAppel", $"ClientPhoneContactFrequency" + $"Appel" + $"PhoneContactFrequency")
      .withColumn("NbrRDV", $"ClientMeetingFrequency" + $"MeetingFrequency" + $"RDV")
      .distinct()
      .drop(
        "ClientPhoneContactFrequency",
        "ClientPhoneContactFrequency",
        "Appel",
        "PhoneContactFrequency",
        "ClientMeetingFrequency",
        "MeetingFrequency",
        "RDV"
      )
      .withColumnRenamed("Ordre", "NbrOrdre")
      .withColumnRenamed("Mail", "NbrMail")
      .withColumnRenamed("Taches", "NbrTaches")



      customerDF
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("src\\GeneratedData\\CustomerRecord")
  }
}


