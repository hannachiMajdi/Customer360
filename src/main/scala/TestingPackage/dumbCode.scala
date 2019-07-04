package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object dumbCode {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ToGraphMigration").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //-----------------Creating customer entity---------------------------------

    val bqClientDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GTI_GeneriquesTiers.csv")

      //Suppression des autres colonnes
      .drop("GTI_CodSociete")
      .drop("GTI_CodPrestataire")
      .drop("EW28_Libelle")
      .drop("GTI_NumStatutPers")
      .drop("GTI_CodTypeInvestisseurMif2")
      .drop("GTI_CodExperienceMif2")
      .drop("GTI_CodCapaciteProduitComplexeMif2")

      //Changement des noms de colonnes
      .withColumnRenamed("GTI_CodTiers", "IdClient")
      .withColumnRenamed("civilite", "Civilite")
      .withColumnRenamed("GTI_DatNaissanceOuCreation", "DatNaissanceOuCreation")
      .withColumnRenamed("yearTmp", "year")
      .withColumnRenamed("csp", "GrpProfession")
      .withColumnRenamed("DepartementResidence", "Agence")
      .withColumn("DatNaissanceOuCreation", $"DatNaissanceOuCreation" cast "Int")


    val crmContactDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CRM_V_CONTACTS_v2.csv")


      //Changement des noms de colonnes
      .withColumnRenamed("Id", "IdContact")
      .withColumnRenamed("code2", "IdClient")
      .withColumnRenamed("Status", "TypeContact")


    //  val customerDF = crmContactDF.join(bqClientDF, crmContactDF.col("IdClient") === bqClientDF.col("IdClient"))
    val customerDF = crmContactDF.join(bqClientDF, "IdClient")


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


    val defaultStation = "missing station"
    val relationGraph = Graph(customerVertices, lienEdges)


    val ranks = relationGraph.pageRank(0.0001).vertices

    ranks
      .join(customerVertices)
      .sortBy(_._2._1, ascending = false) // sort by the rank
      .take(10) // get the top 10
      .foreach(x => println(x._2._2))


  }
}


