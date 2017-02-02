package com.kimtaeyun97.spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._

/**
 * Learning Streaming 
 */
object SparkStreaming {
  def main(args: Array[String]) {
    println("Run")
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming").set("spark.driver.host", "localhost");
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
    println("Done")
    
    
  }
}