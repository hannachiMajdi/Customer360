package MLPackage

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

object Faking {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SingleRecord")
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


    sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\InputRecord_2\\part-00000-eff91192-627c-487f-a4ad-7d24613fe917-c000.csv")
      .saveToEs("ml_customer_record_input")
  }
}


