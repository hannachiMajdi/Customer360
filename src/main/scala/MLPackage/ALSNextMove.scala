package MLPackage

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object ALSNextMove extends Serializable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AttributionDeCredit")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataDF =sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\ALSInput\\part-00000-46140669-62d2-43a0-b8e4-7ec8cbec308c-c000.csv")

    val ratingDF = dataDF
      .select(
        $"UserId".cast("Int"),
        $"MoveId".cast("Int"),
        round($"Rating").cast("Int").as("rating")
      )
      .na.fill(0)


    val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2), seed = 12345L)

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("UserId")
      .setItemCol("MoveId")
      .setRatingCol("rating")
      .setImplicitPrefs(true)

    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    val userRecs = model.recommendForAllUsers(3)
    val df = userRecs.toDF()
      .select($"UserId", explode($"recommendations").alias("recommendation"))
      .select($"UserId", $"recommendation.*")
/*
    userRecs.show(5, false)
    df.show(5, false)
    df.describe().show()

 */
    val tufoDF=dataDF
      .withColumnRenamed("UserId","userIDD")
      .withColumnRenamed("MoveId","moveIDD")
      .withColumnRenamed("Rating","RatingIDD")
    df.join(
      tufoDF
      ,
      tufoDF("userIDD")===df("UserId")&&tufoDF("moveIDD")===df("MoveId")
    ).drop("UserId","MoveId","userIDD","moveIDD","RatingIDD")
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\ALSNextMove")



  }
}


