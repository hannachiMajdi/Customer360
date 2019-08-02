package MLPackage

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Test {

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
      .load("src\\ML\\CustomerRecordInput\\part-00000-c7702e51-86f1-43bc-a630-a58cf6202bb1-c000.csv")
      val customerDf = CustomerDF

      .filter(
        $"CodTiers".isNotNull &&
        $"soldeTitre".isNotNull &&
        $"soldeLiquide".isNotNull &&
        $"Status".isNotNull &&
        $"NomComplet".isNotNull &&
        $"PaysNaissance".isNotNull &&
        $"Civilite".isNotNull &&
        $"DateNaissanceOuCreation".isNotNull &&
        $"Sexe".isNotNull &&
        $"SituationFamiliale".isNotNull &&
        $"GroupProfession".isNotNull &&
        $"Profession".isNotNull &&
        $"DepartementResidence".isNotNull &&
        $"Age".isNotNull &&
        $"Ta".isNotNull &&
        $"Ap".isNotNull &&
        $"Or".isNotNull &&
        $"RD".isNotNull &&
        $"Ma".isNotNull &&
        $"Churn".isNotNull
      )

  }
}


