package DWPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lower
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object DimensionCompte {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("Bridge")
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
      //.load("src\\SourceData\\CLI_TCL_TiersComptesLocal.csv")
      .load("hdfs://localhost:9000/DataLake/CoreBanking/TiersCompte/*.csv")
      .select(
        $"TCL_CodCompte_hash".as("CodCompte")
      )
        .na.drop()
        .distinct()



    DataDF
      .saveToEs("dw_dimension_compte")
      /*.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\DW\\dw_dimension_compte")*/


  }
}


