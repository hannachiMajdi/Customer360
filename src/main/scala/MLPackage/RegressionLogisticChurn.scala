package MLPackage

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml._

object RegressionLogisticChurn {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("AttributionDeCredit")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._

    val CustomerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\InputRecord\\part-00000-a14416ca-3b87-4413-8ff6-eebe4915dd36-c000.csv")
      .withColumnRenamed("Churn","label")
      .na.drop()

    CustomerDF.printSchema()

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


    // assembler

    val assembler = (new VectorAssembler()
      .setInputCols(
        Array(
          "soldeTitre",
          "soldeLiquide",
          "NbrReclamation",
          "ExperienceEnBQ",
          "nbProduit",
          "NbrNantissement",
          "nbrTransaction",
          "SexeVector",
          "SituationFamilialeVector",
          "GroupProfessionVector",
          "ProfessionVector",
          "PaysNaissanceVector",
          "StatusVector"
        ))
      .setOutputCol("features")
      )

    val Array(training,test) = CustomerDF.randomSplit(Array(0.7,0.3),seed = 12345)

    import org.apache.spark.ml.Pipeline

    val lr = new LogisticRegression()

    val pipeline  = new Pipeline().setStages(
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
        lr
      ))

    val model = pipeline.fit(training)

    val results = model.transform(test)

    results.printSchema()

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

      val df = model.transform(CustomerDF)

       .select("CodTiers","probability","prediction")

    // A UDF to convert VectorUDT to ArrayType
    val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )

    // Add a ArrayType Column
    val dfArr = df.withColumn("probabilityArr" , vecToArray($"probability") )

    // Array of element names that need to be fetched
    // ArrayIndexOutOfBounds is not checked.
    // sizeof `elements` should be equal to the number of entries in column `features`
    val elements = Array("Prob_0", "Prob_1")

    // Create a SQL-like expression using the array
    val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("probabilityArr").getItem(idx).as(alias) }

    // Extract Elements from dfArr
    val dDF = dfArr.select((col("*")+: sqlExpr) :_*

    )
      //.saveToEs("dw_dimension_client/client")
    val ff = dDF.select($"CodTiers",
      $"Prob_0",
      $"Prob_1",
      $"prediction"
    ).na.drop().toDF()

      ff.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save("src\\ML\\ChurnPrediction_2")


  }
}


