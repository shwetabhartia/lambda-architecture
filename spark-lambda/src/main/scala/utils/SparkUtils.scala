package utils

import java.lang.management.ManagementFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Shweta on 12/18/2017.
  */
object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName : String) = {
    var checkPointDirectory = ""

    //get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")

    //checking if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
      checkPointDirectory = "file:///F:/Project/temp/"
    } else {
      checkPointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir(checkPointDirectory)
    //sc.setCheckpointDir("file:///F:/Project/temp/")
    sc
  }

  def getSQLContext(sc : SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration : Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
