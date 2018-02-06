package streaming

import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._
    val batchDuration = Seconds(4)

    def streamingApp(sc : SparkContext, batchDuration : Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      //Kafka changes
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopics

      //Kafka Receiver Approach
      /*
      val kafkaParams = Map(
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc,kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
        .map(_._2)
        */

      //Kafka Direct Approach
      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "smallest"
      )

      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaDirectParams, Set(topic)
      ).map(_._2)

      //Batch and Streaming
      /*val inputPath = isIDE match {
        case true => "file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }
      val textDStream = ssc.textFileStream(inputPath)*/

      val activityStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDActivity(input)
      }).cache()

      /*Acivity by product by timestamp hour*/
      val activityStateSpec = StateSpec.function(mapActivityStateFunc).timeout(Minutes(120))

      val statefulActivityByProduct = activityStream.transform(rdd => {
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
      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()

      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a,b) => b,
          (x,y) => x,
          Seconds(30/4*4)
        )
        .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
          .toDF().registerTempTable("ActivityByProduct"))

      //statefulActivityByProduct.print(10)

      /*Unique Visitors by product*/
      val visitorStateSpec = StateSpec.function(mapVisitorsStateFunc).timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map(a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()

      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a,b) => b,
          (x,y) => x,
           Seconds(30/4*4)
        )
        .foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
          .toDF().registerTempTable("VisitorsByProduct"))


        /*updateStateByKey((newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]) => {
       var (prevTimeStamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((System.currentTimeMillis(), 0L, 0L, 0L))
        var result : Option[(Long, Long, Long, Long)] = null

        if (newItemsPerKey.isEmpty) {
          if (System.currentTimeMillis() - prevTimeStamp > 30000 + 4000)
            result = None
          else
            result = Some((prevTimeStamp, purchase_count, add_to_cart_count, page_view_count))
        } else {

          newItemsPerKey.foreach(a => {
            purchase_count += a.purchase_count
            add_to_cart_count += a.add_to_cart
            page_view_count += a.page_view_count
          })

          result = Some((System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))
        }

        result
      })*/

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
