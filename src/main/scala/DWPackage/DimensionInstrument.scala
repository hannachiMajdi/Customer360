package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object DimensionInstrument {

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
      .load("src\\SourceData\\INS_GIN_GeneriqueInstruments.csv")
      .select(
        $"GIN_CodISIN".as("CodISIN"),
        lower($"KW02_Libelle").as("Libelle"),
        lower($"GIN_LibInstrument").as("LibInstrument"),
        lower($"GIN_LibEmetteur").as("LibEmetteur"),
        $"GIN_CodPaysEmission".as("CodPaysEmission")
      )
        .na.drop()
        .distinct()


    DataDF
      //.saveToEs("dw_dimension_instrument/instrument")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_dimension_instrument")
  }
}


