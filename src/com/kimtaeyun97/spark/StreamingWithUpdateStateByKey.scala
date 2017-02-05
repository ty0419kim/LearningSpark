package com.kimtaeyun97.spark

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.log4j._
/**
 * updateStateByKey() Test
 * The values of same key are summed regardless batch
 */
object StreamingWithUpdateStateByKey {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").
      setAppName("StreamingWithUpdateStateByKey").
      set("spark.driver.host", "localhost")
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("../LearningSpark/streamingFiles")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(x => x.split(" "))
    val pairs = words.map(x => (x, 1))
    
    //Either of two below can be used 
    
    //val runningCounts = pairs.updateStateByKey[Int](updateFunction _)
    val runningCounts = pairs.updateStateByKey((newValues: Seq[Int], runningCount: Option[Int]) => {
      var result: Option[Int] = null

      if (newValues.isEmpty) {
        result = Some(runningCount.get)
      } else {
        newValues.foreach(x => {
          result = Some(x + runningCount.getOrElse(0))
        })
      }

      result
    })
    runningCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

  //ref : http://amithora.com/spark-update-by-key-explained/
  def updateFunction(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[(Int)] = {

    var result: Option[(Int)] = null
    if (newValues.isEmpty) { //check if the key is present in new batch if not then return the old values
      result = Some(runningCount.get)
    } else {
      newValues.foreach { x =>
        { // if we have keys in new batch ,iterate over them and add it
          if (runningCount.isEmpty) {
            result = Some(x) // if no previous value return the new one
          } else {
            result = Some(x + runningCount.get) // update and return the value
          }
        }
      }
    }

    result = None
    result
  }
}