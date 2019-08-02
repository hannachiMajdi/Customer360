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
      .setAppName("ToGraphMigration")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._

    val CustomerDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\CustomerRecordInput\\part-00000-c7702e51-86f1-43bc-a630-a58cf6202bb1-c000.csv")

      .filter(
        $"CodTiers".isNotNull && $"CodTiers" =!="null"&&
          $"soldeTitre".isNotNull &&
          $"soldeLiquide".isNotNull &&
          $"Status".isNotNull && $"Status" =!="null"&&
          $"NomComplet".isNotNull && $"NomComplet" =!="null"&&
          $"PaysNaissance".isNotNull && $"PaysNaissance" =!="null"&&
          $"Civilite".isNotNull && $"Civilite" =!="null"&&
          $"DateNaissanceOuCreation".isNotNull && $"DateNaissanceOuCreation" =!="null"&&
          $"Sexe".isNotNull && $"Sexe" =!="null"&&
          $"SituationFamiliale".isNotNull && $"SituationFamiliale" =!="null"&&
          $"GroupProfession".isNotNull && $"GroupProfession" =!="null"&&
          $"Profession".isNotNull && $"Profession" =!="null"&&
          $"DepartementResidence".isNotNull &&
          $"Age".isNotNull &&
          $"Ta".isNotNull &&
          $"Ap".isNotNull &&
          $"Or".isNotNull &&
          $"RD".isNotNull &&
          $"Ma".isNotNull &&
          $"Churn".isNotNull
      )

      .withColumnRenamed("Churn","label")
      .drop("quitter","NomComplet","DateNaissanceOuCreation","Civilite","Status")

      .na.drop()



    CustomerDF.printSchema()


    val SXIndexer = new StringIndexer()
      .setInputCol("Sexe")
      .setOutputCol("SexeCode")

    val SFIndexer = new StringIndexer()
      .setInputCol("SituationFamiliale")
      .setOutputCol("SituationFamilialeCode")

    val GPIndexer = new StringIndexer()
      .setInputCol("GroupProfession")
      .setOutputCol("GroupProfessionCode")

    val PrIndexer = new StringIndexer()
      .setInputCol("Profession")
      .setOutputCol("ProfessionCode")

    // Convert numerival into one hot encoding


    val  SXEncoder = new OneHotEncoder()
      .setInputCol("SexeCode")
      .setOutputCol("SexeVector")


    val  SFEncoder = new OneHotEncoder()
      .setInputCol("SituationFamilialeCode")
      .setOutputCol("SituationFamilialeVector")

    val GPEncoder = new OneHotEncoder()
      .setInputCol("GroupProfessionCode")
      .setOutputCol("GroupProfessionVector")

    val PrEncoder = new OneHotEncoder()
      .setInputCol("ProfessionCode")
      .setOutputCol("ProfessionVector")

    // assembler

    val assembler = (new VectorAssembler()
      .setInputCols(
        Array(
          "soldeTitre",
          "soldeLiquide",
          "Age",
          "Ta",
          "Ap",
          "Or",
          "RD",
          "Ma",

          "SexeVector",
          "SituationFamilialeVector",
          "GroupProfessionVector",
          "ProfessionVector"
        ))
      .setOutputCol("features")
      )

    val Array(training,test) = CustomerDF.randomSplit(Array(0.7,0.3),seed = 12345)

    import org.apache.spark.ml.Pipeline

    val lr = new LogisticRegression()

    val pipeline  = new Pipeline().setStages(
      Array(
     //   PNIndexer,
        SXIndexer.setHandleInvalid("keep"),
        SFIndexer.setHandleInvalid("keep"),
        GPIndexer.setHandleInvalid("keep"),
        PrIndexer.setHandleInvalid("keep"),
   //     PNEncoder,
        SXEncoder,
        SFEncoder,
        GPEncoder,
        PrEncoder,
        assembler,
        lr
      ))

    val model = pipeline.fit(training)

    val results = model.transform(test)

    results.printSchema()

      val df = results

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
      .save("src\\ML\\ChurnPrediction")




  }
}


