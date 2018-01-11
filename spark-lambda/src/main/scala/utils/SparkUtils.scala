package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("Intellij Idea")
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
      checkPointDirectory = "file:///F:/Project/temp"
    } else {
      checkPointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    //sc.setCheckpointDir(checkPointDirectory)
    sc
  }

  def getSQLContext(sc : SparkContext) = {
    val sqLContext = SQLContext.getOrCreate(sc)
    sqLContext
  }
}
