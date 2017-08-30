package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Shweta on 8/11/2017.
  */
object BatchJob {
  def main(args : Array[String]) : Unit = {

    val conf = new SparkConf()
      .setAppName("Lambda with Spark")
      .setMaster("local[*]")

    /*if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("Intellij Idea")) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
    }*/

    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val input: RDD[String] = sc.textFile("file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv")

    val inputRDD = input.map{line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000*60*60
      Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6))
    }

    val keyedByProduct = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()

    /*Visitor By Product per hour*/
    val visitorsByProduct = keyedByProduct
      .mapValues(a => a.visitor)
      .distinct()
      .countByKey()


    /*Activity by product per hour*/
    val activityByProduct = keyedByProduct
      .mapValues{ a =>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
        }
      }
      .reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
