package GraphPackage

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object CreationGraph {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //-----------------Creating customer entity---------------------------------
    val customerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")

      //Changement des noms de colonnes
      .select(
      $"Id".as("IdContact"),
      $"code2".as("IdClient")
    )



    val crmLienDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\ContactRelation_OBP_CRM.csv")
      //Changement des noms de colonnes
      .withColumnRenamed("ContactId origine", "ContactId_origine")
      .withColumnRenamed("ContactId Cible", "ContactId_cible")


    val customerVertices: RDD[(VertexId, String)] =
      customerDF
        .select("IdContact", "IdClient")
        .distinct()
        .rdd
        .map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String])) // maintain type information


    val lienEdges: RDD[Edge[Long]] = crmLienDF
      .select("ContactId_origine", "ContactId_cible")
      .rdd
      .map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue))


    val defaultStation = "Aucun Lien"
    val relationGraph = Graph(customerVertices, lienEdges)


    val ranks = relationGraph.pageRank(0.0001).vertices

    ranks
      .join(customerVertices)
      .sortBy(_._2._1, ascending = false) // sort by the rank
       // get the top 10
      .repartition(1)
      .saveAsTextFile("src\\Graph\\MostConnectedwPageRank")


  }
}


