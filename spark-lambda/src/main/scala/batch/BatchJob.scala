package batch

import config.Settings
import utils.SparkUtils._
import domain._
import org.apache.spark.sql.SaveMode

/**
  * Created by Shweta on 8/11/2017.
  */
object BatchJob {
  def main(args : Array[String]) : Unit = {

    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    val wlc = Settings.WebLogGen

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // initialize input RDD
    val inputDF = sqlContext.read.parquet(wlc.hdfsPath)
      .where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6")

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

    //visitorsByProduct.printSchema()

    /*Activity by product per hour using Dataframe*/
    val activityByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour,
        |SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        |SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
        |FROM activity
        |GROUP BY product, timestamp_hour
      """.stripMargin)

    //activityByProduct.registerTempTable("activityByProduct")
    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }

}
