package MLPackage

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeCredit {
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
        .withColumnRenamed("AttributionCredit","label")

      customerDataDF.printSchema()

      //________________________ Sexe Attribute ____________________________
      val SXIndexer = new StringIndexer()
        .setInputCol("Sexe")
        .setOutputCol("SexeCode")
        .setHandleInvalid("keep")

      val  SXEncoder = new OneHotEncoder()
        .setInputCol("SexeCode")
        .setOutputCol("SexeVector")
      //________________________ SituationFamiliale Attribute ____________________________

      val SFIndexer = new StringIndexer()
        .setInputCol("SituationFamiliale")
        .setOutputCol("SituationFamilialeCode")
        .setHandleInvalid("keep")


      val  SFEncoder = new OneHotEncoder()
        .setInputCol("SituationFamilialeCode")
        .setOutputCol("SituationFamilialeVector")

      //________________________ GroupProfession Attribute ____________________________

      val GPIndexer = new StringIndexer()
        .setInputCol("GroupProfession")
        .setOutputCol("GroupProfessionCode")
        .setHandleInvalid("keep")

      val GPEncoder = new OneHotEncoder()
        .setInputCol("GroupProfessionCode")
        .setOutputCol("GroupProfessionVector")
      //________________________ Profession Attribute ____________________________

      val PrIndexer = new StringIndexer()
        .setInputCol("Profession")
        .setOutputCol("ProfessionCode")
        .setHandleInvalid("keep")

      val PrEncoder = new OneHotEncoder()
        .setInputCol("ProfessionCode")
        .setOutputCol("ProfessionVector")
      //________________________ Status Attribute ____________________________

      val STIndexer = new StringIndexer()
        .setInputCol("Status")
        .setOutputCol("StatusCode")
        .setHandleInvalid("keep")

      val STEncoder = new OneHotEncoder()
        .setInputCol("StatusCode")
        .setOutputCol("StatusVector")

      //________________________ PaysNaissance Attribute ____________________________

      val PNIndexer = new StringIndexer()
        .setInputCol("PaysNaissance")
        .setOutputCol("PaysNaissanceCode")
        .setHandleInvalid("keep")

      val PNEncoder = new OneHotEncoder()
        .setInputCol("PaysNaissanceCode")
        .setOutputCol("PaysNaissanceVector")


      //_________________________ Selection des inputs _____________________________________________

      val featureCols = Array(
        "soldeTitre",
        "soldeLiquide",
        "Age",
        "nbrTransactionMensuel",
        "ExperienceEnBQ",
        "nbProduit",
        "NbrNantissement",
        "SexeVector",
        "SituationFamilialeVector",
        "GroupProfessionVector",
        "ProfessionVector",
        "StatusVector",
        "PaysNaissanceVector")

      val assembler = new VectorAssembler()
        .setInputCols(featureCols)
        .setOutputCol("features")

      // set up a DecisionTreeClassifier estimator
      val dTree = new DecisionTreeClassifier().setLabelCol("label")
        .setFeaturesCol("features")
      val pipeline = new Pipeline()
        .setStages(
          Array(
            PNIndexer,
            SXIndexer,
            SFIndexer,
            GPIndexer,
            PrIndexer,
            STIndexer,

            PNEncoder,
            SXEncoder,
            SFEncoder,
            GPEncoder,
            PrEncoder,
            STEncoder,
            assembler,
            dTree)
        )


      // Search through decision tree's maxDepth parameter for best model
      val paramGrid = new ParamGridBuilder()
        .addGrid(dTree.maxDepth, Array(2,3,4,5,6,7))
        .build()


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

      val predictions = bestModel.transform(test)
      val accuracy = evaluator.evaluate(predictions)
      evaluator.explainParams()
      val result = predictions.select("label", "prediction", "probability")
      result.show(10)
      println(accuracy)

    }
}


