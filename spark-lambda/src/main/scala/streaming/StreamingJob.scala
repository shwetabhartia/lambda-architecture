package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.SparkUtils._


object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("Lambda with Spark")

    val batchDuration = Seconds(4)

    val ssc = new StreamingContext(sc, batchDuration)

    /*val inputPath = isIDE match {
      case true => "file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
      case false => "file:///vagrant/input"
    }*/
    val inputPath = "file:///F:/Project/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"

    val textDStream = ssc.textFileStream(inputPath)
    textDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
