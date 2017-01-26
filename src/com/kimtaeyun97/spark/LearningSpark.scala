package com.kimtaeyun97.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._

object LearningSpark {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("LearningSpark")
    val sc = new SparkContext(conf)
    sc.stop()
    println("sc.stop()")
  }
}