package TestingPackage

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ChurnApplied {


  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("Logistic Regression test")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

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

    val customerDF = customerDataDF.join(churnData,"IdClient")



    // converting strings into numerical
    val geoIndexer = new StringIndexer()
      .setInputCol("Geography")
      .setOutputCol("GeographyCode")
    val genderIndexer = new StringIndexer()
      .setInputCol("Gender")
      .setOutputCol("GenderCode")

    // Convert numerival into one hot encoding
    val genderEncoder = new OneHotEncoder()
      .setInputCol("GenderCode")
      .setOutputCol("GenderVector")

    val cardEncoder = new OneHotEncoder()
      .setInputCol("HasCrCard")
      .setOutputCol("CardVector")

    val activeEncoder = new OneHotEncoder()
      .setInputCol("IsActiveMember")
      .setOutputCol("ActiveVector")

    val geoEncoder = new OneHotEncoder()
      .setInputCol("GeographyCode")
      .setOutputCol("GeographyVector")

    // assembler

    val assembler = (new VectorAssembler()
      .setInputCols(
        Array(
          "CreditScore",
          "Age",
          "Tenure",
          "Balance",
          "NumOfProducts",
          "EstimatedSalary",
          "GenderVector",
          "ActiveVector",
          "GeographyVector"
        ))
      .setOutputCol("features")
      )

    val Array(training, test) = customerDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    import org.apache.spark.ml.Pipeline

    val lr = new LogisticRegression()

    val pipeline = new Pipeline().setStages(
      Array(
        geoIndexer,
        genderIndexer,
        genderEncoder,
        geoEncoder,
        cardEncoder,
        activeEncoder,
        assembler,
        lr
      ))
    val model = pipeline.fit(training)

    val results = model.transform(test)

    results.printSchema()
    ///
    ///Model evaluation

    import org.apache.spark.mllib.evaluation.MulticlassMetrics

    // val predictionAndLabels = results.select("prediction","label")
    val predictionAndLabels = results //select("prediction","label")
      .selectExpr("cast(prediction as double) prediction", "cast(label as double) label")
      // .withColumn("label", $"label" cast "Double")
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predictionAndLabels)

    println("Confusion matrix")
    println(metrics.confusionMatrix)
    println(metrics.precision)

  }
}