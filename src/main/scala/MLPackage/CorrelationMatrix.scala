package MLPackage

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.Row


object CorrelationMatrix {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf()
      .setAppName("SingleRecord")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val customerDataDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load("src\\ML\\InputRecord\\part-00000-a14416ca-3b87-4413-8ff6-eebe4915dd36-c000.csv")
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
          "AttributionCredit",
          "Churn"

        ))
      .setOutputCol("features")
      )
    val df = assembler.transform(customerDataDF)
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head

    println("Pearson correlation matrix:\n ")
    println(coeff1.toString(9,Int.MaxValue))

        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        println("Spearman correlation matrix:\n ")

    println(coeff2.toString(9,Int.MaxValue))


  }
}


