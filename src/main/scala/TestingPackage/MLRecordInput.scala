package TestingPackage

import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object MLRecordInput {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val customerDataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\CustomerRecord\\data.csv")

    val churnData = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\CustomerChurn\\data.csv")

    val soldeData = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\GeneratedData\\CustomerSolde\\data.csv")

    val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val customerDF = customerDataDF
      .join(churnData, "IdClient")
      .join(soldeData,"IdClient")
      .select(
        "DatNaissanceOuCreation",
        "Agence",
        "SituationFamiliale",
        "profession",
        "Sexe",
        "PaysNaissance",
        "NbrOrdre",
        "NbrMail",
        "NbrTaches",
        "NbrAppel",
        "NbrRDV",
        "solde",
        "Churn"
      )
      .withColumn("age", when($"DatNaissanceOuCreation" >= 1900 , abs($"DatNaissanceOuCreation" - Year)).otherwise(65))

      .na.fill(0.0)


    customerDF.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\GeneratedData\\CustomerDataInputML")



  }
}


