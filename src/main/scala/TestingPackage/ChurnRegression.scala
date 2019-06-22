package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature.{VectorAssembler,StringIndexer,VectorIndexer,OneHotEncoder}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
object ChurnRegression {

  /*case class Client(IdClient:String,PaysNaissance:String,Civilite:String,DatNaissanceOuCreation:Int,Sexe:String,SituationFamiliale:String,GrpProfession:String
  ,Profession:String,Agence:String )
  case class Lien(IdCompte:String ,IdClient:String , TypeLien:String)*/

  def main(args : Array[String]): Unit = {
    var conf = new SparkConf()
              .setAppName("Logistic Regression test")
              .setMaster("local[*]")

  //  conf.set("es.index.auto.create", "true")
   // conf.set("es.nodes", "127.0.0.1")
  //  conf.set("es.port","9200")



    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    val customerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load("src\\data\\Churn_Modelling.csv")
      .drop("RowNumber", "CustomerId", "Surname")
      .withColumnRenamed("Exited","label")
      .na.drop()
      //.dropDuplicates()
      .distinct()


    // converting strings into numerical
    val geoIndexer = new StringIndexer()
      .setInputCol("Geography")
      .setOutputCol("GeographyCode")
    val genderIndexer = new StringIndexer()
      .setInputCol("Gender")
      .setOutputCol("GenderCode")

  // Convert numerival into one hot encoding
    val  genderEncoder = new OneHotEncoder()
      .setInputCol("GenderCode")
      .setOutputCol("GenderVector")

    val  cardEncoder = new OneHotEncoder()
      .setInputCol("HasCrCard")
      .setOutputCol("CardVector")

    val  activeEncoder = new OneHotEncoder()
      .setInputCol("IsActiveMember")
      .setOutputCol("ActiveVector")

    val  geoEncoder = new OneHotEncoder()
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

    val Array(training,test) = customerDF.randomSplit(Array(0.7,0.3),seed = 12345)

    import org.apache.spark.ml.Pipeline

    val lr = new LogisticRegression()

    val pipeline  = new Pipeline().setStages(
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
    val predictionAndLabels = results//select("prediction","label")
      .selectExpr("cast(prediction as double) prediction","cast(label as double) label")
     // .withColumn("label", $"label" cast "Double")
        .rdd
       .map(row => (row.getDouble(0), row.getDouble(1)))

          val metrics = new MulticlassMetrics(predictionAndLabels)

    println("Confusion matrix")
    println(metrics.confusionMatrix)
    println(metrics.precision)

  }
}