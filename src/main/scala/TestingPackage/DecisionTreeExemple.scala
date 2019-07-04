package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.feature.VectorAssembler

object DecisionTreeExemple {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._


      val customerDataDF = sqlContext.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .load("src\\SourceData\\Churn_Modelling.csv")
        .drop("RowNumber","CustomerId","Surname")
        .withColumnRenamed("Exited","label")

      val genderIndexer = new StringIndexer()
        .setInputCol("Gender")
        .setOutputCol("GenderIndex")

      val geoIndexer = new StringIndexer()
        .setInputCol("Geography")
        .setOutputCol("GeoraphyIndex")

      val featureCols = Array(
                  "CreditScore",
                  "Age",
                  "Tenure",
                  "Balance",
                  "NumOfProducts",
                  "HasCrCard",
                  "IsActiveMember",
                  "EstimatedSalary")

      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("features")

      // set up a DecisionTreeClassifier estimator
      val dTree = new DecisionTreeClassifier().setLabelCol("label")
        .setFeaturesCol("features")

      // Chain indexers and tree in a Pipeline
      val pipeline = new Pipeline()
        .setStages(
          Array(
            genderIndexer,
            geoIndexer,
            assembler,
            dTree)
        )

      // Search through decision tree's maxDepth parameter for best model
      val paramGrid = new ParamGridBuilder().addGrid(dTree.maxDepth,
        Array(2, 3, 4, 5, 6, 7)).build()

      // Set up Evaluator (prediction, true label)
      val evaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")

      val Array(training,test) = customerDataDF.randomSplit(Array(0.7,0.3),seed = 12345)

      // Set up 3-fold cross validation
      val crossval = new CrossValidator().setEstimator(pipeline)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid).setNumFolds(3)

      val cvModel = crossval.fit(training)

      // Fetch best model
      val bestModel = cvModel.bestModel
      val treeModel = bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
        .stages(3).asInstanceOf[DecisionTreeClassificationModel]
      println("Learned classification tree model:\n" + treeModel.toDebugString)


      val predictions = cvModel.transform(test)
      val accuracy = evaluator.evaluate(predictions)
      evaluator.explainParams()
      val result = predictions.select("label", "prediction", "probability")
      result.show
      println(accuracy)

    }
}


