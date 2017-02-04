package com.kimtaeyun97.spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._
/**
 * Test DStream from the local directory with simple text files
 */
object DStreamWithTextFile {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().
      setMaster("local[*]").
      setAppName("DStreamWithTextFile").
      set("spark.driver.host", "localhost")

    //When using "textFileStream", the name of input file that is moving into the target folder
    //must be the name what have never been in the target folder.
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.textFileStream("../LearningSpark/streamingFiles")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    println("Done")
  }

}