package MLPackage

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object ALSNextMoveDataInput {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("AttributionDeCredit").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val BridgeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_bridge_client_compte\\part-00000-f7a024e9-7641-452b-afd2-57e21469d1b0-c000.csv")

    // ________________Nbr Transaction_____________________

    val SommeDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\DW\\dw_fait_transaction\\part-00000-78951a1c-3a61-42e1-9bbd-7a4d3d6a550b-c000.csv")
      .join(BridgeDF, "FK_CodCompte")
      .groupBy("FK_CodTiers","FK_CodIsin")
      .agg(
        count("*").as("somme")
      )
      .na.fill(0)


    val maxDF = SommeDF
      .groupBy("FK_CodTiers")
      .agg(
        max("somme").as("Max")
      )



    val DataDF = SommeDF.join(maxDF,"FK_CodTiers")
        .withColumn("Rating",$"somme"*5/$"Max")
        .select($"FK_CodTiers",$"FK_CodIsin",$"Rating")


    val UserIDindexer = new StringIndexer()
      .setInputCol("FK_CodTiers")
      .setOutputCol("UserId")

    val MoveIDindexer = new StringIndexer()
      .setInputCol("FK_CodIsin")
      .setOutputCol("MoveId")


    val withUserIDDF = UserIDindexer.fit(DataDF).transform(DataDF)
    val withMoveIDDF = MoveIDindexer.fit(withUserIDDF).transform(withUserIDDF)

    withMoveIDDF.printSchema()
    withMoveIDDF
      .select(
        $"UserId".cast("Int"),
        $"MoveId".cast("Int"),
        $"Rating".cast("Int").as("MoveRating")
      )


      //.saveToEs("dw_dimension_client/client")
      .repartition(1)
      .write
     /* .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\ALSInput")

      */
  }
}


