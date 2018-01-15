package streaming

import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils.SparkUtils._


object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._
    val batchDuration = Seconds(4)

    def streamingApp(sc : SparkContext, batchDuration : Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = isIDE match {
        case true => "file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)
      val activityStream = textDStream.transform(input => {
        input.flatMap{line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000*60*60
          if(record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      })

      activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """SELECT product, timestamp_hour,
            |SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
            |SUM(CASE WHEN action = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
            |SUM(CASE WHEN action = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
            |FROM activity
            |GROUP BY product, timestamp_hour
          """.stripMargin)
        activityByProduct.map{r => ((r.getString(0), r.getLong(1)),
          ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          )}
      }).print()

      textDStream.print()
      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
