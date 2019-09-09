package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{lower, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object FaitInteraction {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("FaitInteraction")
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

    //src\TargetData\RefProduit
    val DataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      //.load("src\\SourceData\\CRM_V_INTERACTIONSOBP.csv")
      .load("hdfs://localhost:9000/DataLake/CRM/Interaction/*.csv")

      .join(
        sqlContext.read.format("csv")
          .option("header", "true")
          .option("delimiter", ";")
          .option("inferSchema", "true")
          //.load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")
          .load("hdfs://localhost:9000/DataLake/CRM/Contact/*.csv")
          .select(
            lower($"Id").as("contactId"),
            $"code2".as("FK_CodTiers")

          ),"contactId"
      )
      .select(
        $"FK_CodTiers",
        $"ActivityType",
       // lower($"CRO_Dateffet").as("Dateffet"),
        substring_index(lower(col("ActivityDate")), " ", 1).as("date")
      )
        .withColumn("FK_Date",regexp_replace($"date" , lit("-"), lit("" )))
      .withColumn("FK_Activity",expr("substring(ActivityType, 1, 2)"))
      .filter($"FK_CodTiers".isNotNull && $"FK_CodTiers"=!="NULL" )
       .na.drop()
        .drop("date","ActivityType")




    DataDF
      .saveToEs("dw_fait_interaction")
     /* .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_fait_interaction")

      */
  }
}


