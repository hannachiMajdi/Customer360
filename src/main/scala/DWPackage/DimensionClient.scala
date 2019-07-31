package DWPackage

import org.apache.spark.sql.{DataFrame, SQLContext,functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.functions.lower

object DimensionClient {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("ToGraphMigration")
      .setMaster("local[*]")
    /*  .set("es.index.auto.create", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.es.net.ssl","true")
      .set("spark.es.nodes",  "aed8cb3e21e0419d81fe0e71bcff6ed8.eu-central-1.aws.cloud.es.io")
      .set("spark.es.port", "9243")
      .set("spark.es.net.http.auth.user","elastic")
      .set("spark.es.net.http.auth.pass", "jmYf8ihvwQBMbF9S7HRdfouf")
      //.set("spark.es.resource", indexName)
      .set("spark.es.nodes.wan.only", "true")*/

    val sc = new SparkContext(conf)


    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //src\TargetData\RefProduit
    val DataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
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
        .load("src\\SourceData\\names\\names.csv")
        .join(sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv"), "GTI_CodTiers"), "GTI_CodTiers")
      .filter($"GTI_CodTiers".isNotNull)
      .select(
        lower($"GTI_CodTiers").as("CodTiers"),
        lower($"Status").as("Status"),
        lower($"NomComplet").as("NomComplet"),
        lower($"PaysNaissance").as("PaysNaissance"),
        lower($"civilite").as("Civilite"),
        lower($"GTI_DatNaissanceOuCreation").as("DateNaissanceOuCreation"),
        lower($"Sexe").as("Sexe"),
        lower($"SituationFamiliale").as("SituationFamiliale"),
        lower($"csp").as("GroupProfession"),
        lower($"profession").as("Profession"),
        lower($"DepartementResidence").as("DepartementResidence")
      )
        .na.drop()
        .distinct()
    DataDF.printSchema()
    DataDF.describe().show()


    DataDF
      //.saveToEs("dw_dimension_client/client")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_dimension_client")
  }
}


