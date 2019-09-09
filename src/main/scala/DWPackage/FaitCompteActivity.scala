package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{lower, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._


object FaitCompteActivity {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("FaitCompteActivity")
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
     // .load("src\\SourceData\\MEN_GCO_GeneriquesComptes.csv")
      .load("hdfs://localhost:9000/DataLake/CoreBanking/HistoCompte/*.csv")

      .select(

        lower(concat($"GCO_AnMois".cast("String"),lit("01"))).as("FK_Date"),
        $"GCO_CodCompte".as("FK_CodCompte"),
        $"GCO_CodProduit".as("CodProduit"),
        $"GCO_IsOuvert".as("IsOuvert"),
        $"GCO_IsAsv".as("IsAsv"),
        $"GCO_IsNanti".as("IsNanti"),
        $"GCO_LibEtatCompte"
       // lower($"CRO_Dateffet").as("Dateffet"),
      )
      .withColumn("FK_CodProduit",regexp_replace($"CodProduit" , lit("NULL"), lit("cbcc" )))
      .withColumn("LibEtatCompte",regexp_replace($"GCO_LibEtatCompte" , lit("NULL"), lit("Non defini" )))
      .drop("CodProduit","GCO_LibEtatCompte")
        .na.fill(0)





    DataDF
      .saveToEs("dw_fait_compteactivity")
     /* .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_fait_compteactivity")


      */
  }
}


