package MLPackage

import org.apache.spark.ml._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object KmeansSegmentation {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("ToGraphMigration")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._

    val CustomerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\Input\\part-00000-57db80a3-ea18-4e36-b909-03010c5f8702-c000.csv")

      .filter(
        $"CodTiers".isNotNull && $"CodTiers" =!="null"&&
          $"soldeTitre".isNotNull &&
          $"soldeLiquide".isNotNull &&
          $"Status".isNotNull && $"Status" =!="null"&&
          $"NomComplet".isNotNull && $"NomComplet" =!="null"&&
          $"Civilite".isNotNull && $"Civilite" =!="null"&&
          $"Sexe".isNotNull && $"Sexe" =!="null"&&
          $"SituationFamiliale".isNotNull && $"SituationFamiliale" =!="null"&&
          $"GroupProfession".isNotNull && $"GroupProfession" =!="null"&&
          $"Profession".isNotNull && $"Profession" =!="null"&&
          $"DepartementResidence".isNotNull &&
          $"Age".isNotNull &&
          $"NbrReclamation".isNotNull &&
          $"Ap".isNotNull &&
          $"Or".isNotNull &&
          $"RD".isNotNull &&
          $"Ma".isNotNull &&
          $"Churn".isNotNull
      )

      .withColumnRenamed("Churn","label")
      .drop("quitter","NomComplet","DateNaissanceOuCreation","Civilite","Status")

      .na.drop()





  }
}


