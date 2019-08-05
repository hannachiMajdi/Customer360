package MLPackage

import java.util.Calendar

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{abs, count, max, min, substring}
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



    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val faitCompteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_compteactivity\\part-00000-f38cec7f-1dfc-405c-812a-5cdd737d7562-c000.csv")

    val MinDateFaitDF = faitCompteDF
      .select(
        $"FK_CodCompte".as("CodCompte"),
        $"FK_Date"
      )
      .groupBy("CodCompte")
      .agg(
        min("FK_Date").as("MinDate")
      )

    val BridgeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-2a7a157a-1272-475a-9607-4f07afab5903-c000.csv")


   /* val ClientCompteInfoDF = BridgeDF
      .join(
        faitCompteDF
          .join(MinDateFaitDF,
            MinDateFaitDF("CodCompte")===faitCompteDF("FK_CodCompte")
              &&
              MinDateFaitDF("MinDate")===faitCompteDF("FK_Date")
          )
          .select(
            $"FK_CodCompte",
            substring($"FK_Date".cast("String"),1,4).as("DateAjout")
          )
          .withColumn("ExperienceEnAnnee", abs($"DateAjout" - Year))
          .drop("DateAjout").na.fill(0)
        , "FK_CodCompte"
      )
      .groupBy("FK_CodTiers")
      .agg(
        max("ExperienceEnAnnee").as("ExperienceEnBQ"),
        count("FK_CodCompte").as("nbProduit"))

    */

    BridgeDF .groupBy("FK_CodTiers")
      .agg(
        count("FK_CodTiers").as("nbProduit")).describe().show()
  }
}


