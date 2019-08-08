package MLPackage

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object KmeansSegmentation {
  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("AttributionDeCredit").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val customerDataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\InputRecord\\part-00000-a14416ca-3b87-4413-8ff6-eebe4915dd36-c000.csv")
      .withColumn("Solde", $"soldeTitre" + $"soldeLiquide")

    customerDataDF.printSchema()


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
      .setWithStd(true)
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
    println("Cluster Centers: ")
    predictions.printSchema()
    predictions.show(5, false)
    // Shows the result.
    println("Cluster Centers: ")
    println(model.stages(2).asInstanceOf[KMeansModel].clusterCenters.getClass)
    model.stages(2).asInstanceOf[KMeansModel].clusterCenters.foreach(println)

    predictions.select($"CodTiers",$"prediction")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\KmeansSegmentation")

  }
}


