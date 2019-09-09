package MLPackage

import org.apache.spark.ml.{Pipeline, linalg}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.{SparkConf, SparkContext}

object KmeansSegmentation {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("AttributionDeCredit").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val customerDataDF =sqlContext.read.format("org.elasticsearch.spark.sql").load("ml_customer_record_input")

    /*sqlContext.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("inferSchema", "true")
    .load("src\\ML\\InputRecord_2\\part-00000-eff91192-627c-487f-a4ad-7d24613fe917-c000.csv")
      */

    .withColumn("Solde", $"soldeTitre" + $"soldeLiquide")
    .drop("churn","AttributionCredit")
    .na.drop()



    //_________________________ Selection des inputs _____________________________________________

    val featureCols = Array(
      "Solde",
      "Age",
      "nbrTransactionMensuel",
      "ExperienceEnBQ",
      "nbProduit",
      "NbrReclamation",
      "NbrNantissement")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")
    /*
          val normalizer = new Normalizer()
            .setInputCol("features")
            .setOutputCol("normFeatures")
            .setP(1.0)
     */
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(false)
      .setWithMean(true)

    // Trains a k-means model.
    val kmeans = new KMeans().setK(3).setSeed(1L)
      //.setFeaturesCol("normFeatures")
      .setFeaturesCol("scaledFeatures")
    val pipeline = new Pipeline()
      .setStages(
        Array(
          assembler,
          //  normalizer,
          scaler,
          kmeans)
      )

    val model = pipeline.fit(customerDataDF)

    // Make predictions
    val predictions = model.transform(customerDataDF)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

       // Shows the result.
       println("Les centres de clusters: ")
       model.stages(2).asInstanceOf[KMeansModel].clusterCenters.foreach(println)
    /*
           predictions.select($"CodTiers",$"prediction")
             .repartition(1)
             .write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             .option("delimiter", ";")
             .save("src\\ML\\KmeansSegmentation")

*/
           val df = predictions

             .select("CodTiers","scaledFeatures","prediction")

           // A UDF to convert VectorUDT to ArrayType
           val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )

           // Add a ArrayType Column
           val dfArr = df.withColumn("scaledFeaturesArr" , vecToArray($"scaledFeatures") )

           // Array of element names that need to be fetched
           // ArrayIndexOutOfBounds is not checked.
           // sizeof `elements` should be equal to the number of entries in column `features`
           val elements = Array(
             "Scaled_Solde",
             "Scaled_Age",
             "Scaled_nbrTransactionMensuel",
             "Scaled_ExperienceEnBQ",
             "Scaled_nbProduit",
             "Scaled_NbrReclamation",
             "Scaled_NbrNantissement"
           )

           // Create a SQL-like expression using the array
           val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("scaledFeaturesArr").getItem(idx).as(alias) }

           // Extract Elements from dfArr
           val dDF = dfArr.select((col("*")+: sqlExpr) :_*)
           //.saveToEs("dw_dimension_client/client")
           dDF.select(
             $"CodTiers",
             $"Scaled_Solde",
             $"Scaled_Age",
             $"Scaled_nbrTransactionMensuel",
             $"Scaled_ExperienceEnBQ",
             $"Scaled_nbProduit",
             $"Scaled_NbrReclamation",
             $"Scaled_NbrNantissement",
             $"prediction"
           ).na.drop().toDF()
             .saveToEs("ml_segmentation")
           /*  .repartition(1)
             .write
             .format("com.databricks.spark.csv")
             .option("header", "true")
             .option("delimiter", ";")
             .save("src\\ML\\KmeansSegmentation_0")


            */
  }
}


