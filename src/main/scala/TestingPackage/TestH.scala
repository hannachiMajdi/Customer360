package TestingPackage

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object TestH {
    def main(args: Array[String]): Unit = {

      var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")

      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val sqlContext = new SQLContext(sc)


      println("Hello")
    }
}


