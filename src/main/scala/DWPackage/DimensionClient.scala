package DWPackage

import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.functions.{abs, round, when}

object DimensionClient {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("DimCLient")
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
    //src\TargetData\RefProduit
    val DataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
//      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
      .load("hdfs://localhost:9000/DataLake/CRM/Contact/*.csv")

      .select(

        $"Id".as("CodContact"),
        $"code2".as("GTI_CodTiers"),
        $"Status",
        $"ClientPhoneContactFrequency",
        $"ClientMeetingFrequency",
        $"MeetingFrequency",
        $"PhoneContactFrequency"
      )
      .join(sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        //.load("src\\SourceData\\names\\names.csv")
          .load("hdfs://localhost:9000/DataLake/Other/Names/*.csv")
        .join(sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
         // .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv")
          .load("hdfs://localhost:9000/DataLake/CoreBanking/Tiers/*.csv")
          , "GTI_CodTiers"), "GTI_CodTiers")
      .filter($"GTI_CodTiers".isNotNull)
      .select(
        $"GTI_CodTiers".as("CodTiers"),
        $"Status".as("Status"),
        $"NomComplet".as("NomComplet"),
        $"PaysNaissance".as("PaysNaissance"),
        $"civilite".as("Civilite"),
        $"GTI_DatNaissanceOuCreation".as("DateNaissanceOuCreation"),
        $"Sexe",
        $"SituationFamiliale".as("SituationFamiliale"),
        $"csp".as("GroupProfession"),
        $"profession".as("Profession"),
        $"DepartementResidence".as("DepartementResidence")
      )
      .withColumn("Age", when($"DateNaissanceOuCreation" >= 1900 , abs($"DateNaissanceOuCreation" - Year)).otherwise(65))
      .withColumn("PaysDeNaissance", when($"PaysNaissance".isNull or $"PaysNaissance" === "NULL", "france").otherwise($"PaysNaissance"))
      .withColumn("DatNaissance", when($"DateNaissanceOuCreation".isNull or $"DateNaissanceOuCreation" === "NULL", 1980).otherwise($"DateNaissanceOuCreation"))
      .withColumn("Politesse", when($"Civilite".isNull or $"Civilite" === "NULL", "autre").otherwise($"Civilite"))
      .withColumn("Genre", when($"Sexe".isNull or $"Sexe" === "NULL", "autre").otherwise($"Sexe"))
      .withColumn("SituationFamilial", when($"SituationFamiliale".isNull or $"SituationFamiliale" === "NULL", "autre").otherwise($"SituationFamiliale"))
      .withColumn("GroupeProfession", when($"GroupProfession".isNull or $"GroupProfession" === "NULL", "autre").otherwise($"GroupProfession"))
      .withColumn("Fonction", when($"Profession".isNull or $"Profession" === "NULL", "autre").otherwise($"Profession"))
      .drop("PaysNaissance", "Civilite", "Sexe", "SituationFamiliale", "GroupProfession", "Profession","DateNaissanceOuCreation")
      .na.drop()
      .distinct()
    //DataDF.printSchema()
   // DataDF.describe().show()

    DataDF.saveToEs("dw_dimension_client")
   /* DataDF
        .join(sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("SourceData\\client\\part-00000-8538c3ab-74e9-499a-85a4-971779656e1c-c000.csv")
            .select($"FK_CodTiers".as("CodTiers")).distinct()
          , "CodTiers")

     /* .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_dimension_client")

      */

    */
  }
}


