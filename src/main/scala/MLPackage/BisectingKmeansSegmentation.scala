package MLPackage

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{BisectingKMeans, BisectingKMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object BisectingKmeansSegmentation {
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
      .setWithMean(false)

    // Trains a k-means model.
    val bkm = new BisectingKMeans().setK(4).setSeed(1)
      //.setFeaturesCol("normFeatures")
      .setFeaturesCol("scaledFeatures")
    val pipeline = new Pipeline()
      .setStages(
        Array(
          assembler,
          //  normalizer,
          scaler,
          bkm)
      )

    val model = pipeline.fit(customerDataDF)

    // Make predictions
    val predictions = model.transform(customerDataDF)
    // Evaluate clustering.

    val cost = model.stages(2).asInstanceOf[BisectingKMeansModel].computeCost(predictions)
    println(s"Within Set Sum of Squared Errors = $cost")

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
    model.stages(2).asInstanceOf[BisectingKMeansModel].clusterCenters.foreach(println)
  }
}


