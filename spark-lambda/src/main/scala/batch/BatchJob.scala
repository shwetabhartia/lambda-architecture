package batch

import utils.SparkUtils._
import domain.Activity
import org.apache.spark.rdd.RDD

/**
  * Created by Shweta on 8/11/2017.
  */
object BatchJob {
  def main(args : Array[String]) : Unit = {

    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val input: RDD[String] = sc.textFile("file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv")

    val inputDF = input.flatMap{line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000*60*60
      if(record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    sqlContext.udf.register("UnderExposed",(pageViewCount: Long, purchaseCount: Long) => if (purchaseCount == 0) 0 else pageViewCount / purchaseCount)

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour")/1000),1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    /*Visitor by product using Dataframe*/
    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT(visitor)) AS unique_visitors
        | FROM activity
        | GROUP BY product, timestamp_hour
      """.stripMargin)

    visitorsByProduct.printSchema()

    /*Activity by product per hour using Dataframe*/
    val activityByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour,
        |SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        |SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
        |FROM activity
        |GROUP BY product, timestamp_hour
      """.stripMargin)

    activityByProduct.registerTempTable("activityByProduct")

    val underExposedProducts = sqlContext.sql(
      """SELECT product, timestamp_hour, UnderExposed(page_view_count, purchase_count) as negative_exposure
         FROM activityByProduct
         ORDER BY negative_exposure DESC
         LIMIT 5
      """.stripMargin)

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }

}
