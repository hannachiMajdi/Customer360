package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{lower, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object FaitSoldeActivity {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("ToGraphMigration")
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
      //.load("src\\SourceData\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")
      .load("hdfs://localhost:9000/DataLake/CoreBanking/HistoExtrait/*.csv")

      .select(
        $"PGH_CodCompte".as("FK_CodCompte"),
        lower($"PGH_MntTotalValorisationTitresEnEuros").as("MntTotalValorisationTitresEnEuros"),
        lower($"PGH_MntSoldeEuroEnEuros").as("MntSoldeEuroEnEuros"),
       // lower($"CRO_Dateffet").as("Dateffet"),
        substring_index(lower(col("PGH_DatArrete")), " ", 1).as("date")
      )
        .withColumn("FK_Date",regexp_replace($"date" , lit("-"), lit("" )))
       .na.drop()
        .drop("date")




    DataDF
      .saveToEs("dw_fait_soldeactivity")
      /*.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_fait_soldeactivity")

       */
  }
}


