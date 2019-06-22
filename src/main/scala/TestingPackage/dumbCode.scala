package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer, OneHotEncoder}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object dumbCode {
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
      .load("src\\GeneratedData\\CustomerDataInputML\\data.csv")
      .withColumnRenamed("churn", "label")
      .withColumn("NbrAppels", $"NbrAppel".cast("Int"))
      .withColumn("SituationFamilial",
        when(
          $"SituationFamiliale" === "NULL" or $"SituationFamiliale" === "null" or $"SituationFamiliale".isNull, "OTHER").otherwise($"SituationFamiliale"))
      .drop("DatNaissanceOuCreation", "NbrAppel", "SituationFamiliale", "Agence")
      .na.drop()
      .distinct()

    customerDataDF.printSchema()





    // converting strings into numerical

    val sFIndexer = new StringIndexer()
      .setInputCol("SituationFamilial")
      .setOutputCol("SituationFamilialeCode")

    val proIndexer = new StringIndexer()
      .setInputCol("profession")
      .setOutputCol("ProfessionCode")

    val sexeIndexer = new StringIndexer()
      .setInputCol("Sexe")
      .setOutputCol("SexeCode")

    // Convert numerival into one hot encoding
    /*     val AgenceEncoder = new OneHotEncoder()
           .setInputCol("Agence")
           .setOutputCol("AgenceVector")*/

    val sFEncoder = new OneHotEncoder()
      .setInputCol("SituationFamilialeCode")
      .setOutputCol("SFVector")

    val SexeEncoder = new OneHotEncoder()
      .setInputCol("SexeCode")
      .setOutputCol("SexeVector")
    /*
                val proEncoder = new OneHotEncoder()
                  .setInputCol("ProfessionCode")
                  .setOutputCol("ProfessionVector")
    */


    // assembler

    val assembler = (new VectorAssembler()
      .setInputCols(
        Array(
          "solde",
          "age",
          "NbrOrdre",
          "NbrMail",
          "NbrTaches",
          "NbrRDV",
          "NbrAppels",
          "SexeVector",
          "SFVector"
          //   "AgenceVector",
          //    "ProfessionVector"
        ))
      .setOutputCol("features")
      )

    val Array(training, test) = customerDataDF.randomSplit(Array(0.7, 0.3), seed = 12345)

    import org.apache.spark.ml.Pipeline

    val lr = new LogisticRegression()

    val pipeline = new Pipeline().setStages(
      Array(
        sFIndexer,
        proIndexer,
        sexeIndexer,
        // AgenceEncoder,
        sFEncoder,
        SexeEncoder,
        //   proEncoder,
        assembler,
        lr
      ))
    val model = pipeline.fit(training)

    val results = model.transform(test)

    results.printSchema()
    results.select("prediction", "label").show(60)
    ///
    ///Model evaluation

    import org.apache.spark.mllib.evaluation.MulticlassMetrics

    // val predictionAndLabels = results.select("prediction","label")
    val predictionAndLabels = results //select("prediction","label")
      .selectExpr("cast(prediction as double) prediction", "cast(label as double) label")
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val metrics = new MulticlassMetrics(predictionAndLabels)

    println("Confusion matrix")
    println(metrics.confusionMatrix)
    println(metrics.precision)


  }
}


