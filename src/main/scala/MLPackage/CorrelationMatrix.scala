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
      .load("src\\ML\\InputRecord_2\\part-00000-eff91192-627c-487f-a4ad-7d24613fe917-c000.csv")
      .na.drop()

    val assembler = (new VectorAssembler()
      .setInputCols(
        Array(
          "soldeTitre",
          "soldeLiquide",
          "NbrReclamation",
          "ExperienceEnBQ",
          "nbProduit",
          "NbrNantissement",
          "nbrTransaction"


        ))
      .setOutputCol("features")
      )
    val df = assembler.transform(customerDataDF)
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head

    println("Matrice de correlation Pearson:\n ")
    println(coeff1.toString(9,Int.MaxValue))

        val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
        println("Matrice de correlation de Spearman :\n ")

    println(coeff2.toString(9,Int.MaxValue))


  }
}


