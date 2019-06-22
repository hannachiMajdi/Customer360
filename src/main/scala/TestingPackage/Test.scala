package TestingPackage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
object Test {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._

/*
       val extraitDF= sqlContext.read.format("csv")
         .option("header", "true")
         .option("delimiter",";")
         .option("inferSchema", "true")
         .load("src\\data\\HIS_POH_PositionHisto__extrait.csv")

         //Suppression des autres colonnes
         .drop("POH_CodSociete")
         .drop("POH_CodPrestataire")


         //Changement des noms de colonnes
         .withColumnRenamed("POH_CodCompte","IdCompte")
         .withColumnRenamed("POH_DatArrete","DateExtrait")
         .withColumnRenamed("POH_QteTitres","SoldeTitres")
         .withColumnRenamed("POH_CodIsin","CodIsin")
         .withColumnRenamed("POH_MntValorisationTitresEnEuros","SoldeEnEuro")

      extraitDF.printSchema()
      println(extraitDF.select("IdCompte").distinct().count())
     */

      val histoExtraitDF= sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",";")
        .option("inferSchema", "true")
        .load("src\\data\\HIS_PGH_PositionGeneriquesHisto__20180101+.csv")

        //Suppression des autres colonnes
        .drop("PGH_CodSociete")



        //Changement des noms de colonnes
        .withColumnRenamed("PGH_CodCompte","IdCompteS")
        .withColumnRenamed("PGH_DatArrete","DateExtraitS")
        .withColumnRenamed("PGH_MntTotalValorisationTitresEnEuros","SoldeTitres")
        .withColumnRenamed("PGH_MntSoldeEuroEnEuros","SoldeEnEuro")

  //    histoExtraitDF.select("DateExtrait").orderBy(desc("DateExtrait")).na.drop().show(10)
    //  histoExtraitDF.select("DateExtrait").orderBy(desc("DateExtrait")).na.drop().distinct().show(10)
      val datesDF = histoExtraitDF.select("IdCompteS","DateExtraitS")
        .groupBy("IdCompteS").agg(max("DateExtraitS") as "DateExtrait")
      .distinct()
      .withColumnRenamed("IdCompteS","IdCompte")
      //.withColumnRenamed("DateExtrait","DateExtrait")



       val soldesDF = histoExtraitDF
        .join(datesDF,
          (datesDF("IdCompte") === histoExtraitDF("IdCompteS")) && (datesDF("DateExtrait") === histoExtraitDF("DateExtraitS"))
                , "inner" )
           .distinct()

  soldesDF
      .select("IdCompte","DateExtrait","SoldeEnEuro","SoldeTitres")
        .repartition(1)
        .write
    .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter",";")
        .save("Soldes")
      /*
      // chargement du fichier relation entre tiers et comptes

            val lienDF= sqlContext.read.format("csv")
                .option("header", "true")
                .option("delimiter",";")
                .option("inferSchema", "true")
                .load("src\\data\\CLI_TCL_TiersComptesLocal.csv")

            //Suppression des autres colonnes
              .drop("TCL_CodSociete")
              .drop("TCL_NumLien")

              //Changement des noms de colonnes
              .withColumnRenamed("TCL_CodTiers","IdClient")
              .withColumnRenamed("TCL_CodCompte_hash","IdCompte")
              .withColumnRenamed("CRT_Libelle","TypeLien")
      lienDF.cache()
      val customerDF= sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter",",")
        .option("inferSchema", "true")
        .load("fullcustomersInfo\\data.csv")
      customerDF.cache()
      val dataDF = lienDF.join(customerDF,"IdClient")
                    .join(histoExtraitDF,"IdCompte")
      dataDF.cache()
      println(dataDF.select("IdCompte").distinct().count())
      println(dataDF.select("IdClient").distinct().count())
      println(dataDF.count())*/
    }
}


