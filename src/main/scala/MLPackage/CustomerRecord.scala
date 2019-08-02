package MLPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lower
import org.apache.spark.{SparkConf, SparkContext}

object CustomerRecord {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("ToGraphMigration")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._

    val CompteDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\SourceData\\CLI_GCO_GeneriquesComptes.csv")

    val BridgeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-2a7a157a-1272-475a-9607-4f07afab5903-c000.csv")

    val faitSoldeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_soldeactivity\\part-00000-2c90d107-75dc-4d66-a6fb-23d88212d261-c000.csv")

    val MaxDateFaitDF =  faitSoldeDF
      .select(
        $"FK_CodCompte".as("CodCompte"),
        $"FK_Date"
      )
      .groupBy("CodCompte")
      .agg(
        max("FK_Date").as("MaxDate")
      )
    val PersonalInfoDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_dimension_client\\part-00000-61d60640-1384-4c4b-80b7-25f2f55e8c67-c000.csv")

    val ChurnDF = CompteDF
      .select(
        $"GCO_CodCompte".as("FK_CodCompte"),
        $"GCO_IsOuvert"
      )
      .join(
        BridgeDF
          .select(
            $"FK_CodCompte",
            $"FK_CodTiers"
          )
        , "FK_CodCompte"
      )
      .join(
        faitSoldeDF
          .join(MaxDateFaitDF,
            MaxDateFaitDF("CodCompte")===faitSoldeDF("FK_CodCompte")
            &&
              MaxDateFaitDF("MaxDate")===faitSoldeDF("FK_Date")
          )
          .select(
            $"FK_CodCompte",
            $"MntTotalValorisationTitresEnEuros",
            $"MntSoldeEuroEnEuros"
          )
        , "FK_CodCompte"
      )

      .groupBy("FK_CodTiers")
      .agg(
        sum("GCO_IsOuvert").as("quitter"),
        sum("MntTotalValorisationTitresEnEuros").as("soldeTitre"),
        sum("MntSoldeEuroEnEuros").as("soldeLiquide")
      )
      .withColumn("Churn", when($"quitter" === 0, 1).otherwise(0))




    val crmInteractionDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_interaction\\part-00000-7779f9f9-97ed-4b21-957d-ca0afd90aafd-c000.csv")

    val typeActDF = crmInteractionDF.select("FK_Activity").distinct().collect().map(_(0)).toList

    val pivotDF = crmInteractionDF
      .groupBy("FK_CodTiers","FK_Activity")
      .agg(count("FK_CodTiers").as("count"))
      .groupBy("FK_CodTiers")
      .pivot("FK_Activity", typeActDF)
      .sum("count")
      .na.fill(0)
      .withColumnRenamed("FK_CodTiers","CodTiers")

    val DataDF = ChurnDF
      .withColumnRenamed("FK_CodTiers","CodTiers")
      .join(PersonalInfoDF,"CodTiers")
      .join(pivotDF,"CodTiers")

    DataDF
        //.saveToEs("dw_dimension_client/client")
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save("src\\ML\\CustomerRecordInput")



  }
}


