package com.missArthas.SparkLearning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shhuang on 2017/3/2.
  */
object SparkSort {
  def main(args: Array[String]): Unit = {
    val logFile = "data/words.txt" // Should be some file on your server.
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(logFile, 2).cache()

    val countedData = srcData.map(line => (line.toLowerCase(), 1)).reduceByKey((a, b) => a + b)
    //for debug use
    //countedData.foreach(x => println(x))
    val sortedData = countedData.map { case (k, v) => (v, k) }.sortByKey(false)
    val topKData = sortedData.take(3).map { case (v, k) => (k, v) }
    topKData.foreach(println)
  }

}
