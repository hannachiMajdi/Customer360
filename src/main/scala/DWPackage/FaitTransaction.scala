package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object FaitTransaction {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("ToGraphMigration")
      .setMaster("local[*]")
     /* .set("es.index.auto.create", "true")
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
      .load("src\\SourceData\\CRO_CRO_CROD.csv")
      .select(
        $"CRO_CodCompte".as("FK_CodCompte"),
        lower($"CRO_CodOperation").as("FK_CodOperation"),
        $"CRO_CodIsin".as("FK_CodIsin"),
        lower($"CRO_Qte").as("Qte"),
        lower($"CRO_MntBrutDevDep").as("MntBrutDevDep"),
       // lower($"CRO_Dateffet").as("Dateffet"),
        substring_index(lower(col("CRO_Dateffet")), " ", 1).as("date")
      )
        .withColumn("FK_Date",regexp_replace($"date" , lit("-"), lit("" )))
       .na.drop()
        .drop("date")




    DataDF
      //.saveToEs("dw_fait_transaction/transaction")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_fait_transaction")
  }
}


