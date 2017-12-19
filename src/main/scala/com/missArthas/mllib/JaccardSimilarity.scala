package com.missArthas.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


class JaccardSimilarity extends java.io.Serializable{

  /**
    * 计算item的相关item
    * @param data (userID, itemID)不重复
    * @return
    */
  def simMatrix(data: DataFrame, itemNumStrict:(Int, Int)) :RDD[((Int, Int), Double)] = {
    //DataFrame转化成RDD
    val dataRDD = data.rdd.map{case Row(userID: Int, itemID: Int, score: Double) => (userID, itemID, score)}

    //jaccard相似度，交集／并集
    val itemSimMatrix = simMatrix(dataRDD, itemNumStrict)

    itemSimMatrix
  }

  def simItems(data: DataFrame, itemNumStrict:(Int, Int), topk: Int=10) = {
    //笛卡尔积之后，item之间的相似度
    val itemSimMatrix = simMatrix(data, itemNumStrict)

    //给出每种item的最相似的topk
    val itemSimTopK = itemSimMatrix.map{ case((id1, id2), score) => (id1, Seq((id2, score)))}
      .reduceByKey((a, b) => a.union(b))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(topk)) }

    itemSimTopK
  }

  def simMatrix(data: RDD[(Int, Int, Double)], itemNumStrict:(Int, Int)) :RDD[((Int, Int), Double)] = {
    //对item有过记录的user集合
    val itemToUser = data.map{
      case (userID, itemID, score) => (itemID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))

    //笛卡尔积,大量运算
    val cartesianResult = itemToUser cartesian itemToUser

    //jaccard相似度，交集／并集
    //(s._1._1, s._2._1)是物品id对
    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toDouble / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    itemSimMatrix
  }

  def simChosed(itemList: DataFrame, ratings: DataFrame): DataFrame = {
    null
  }

}

object JaccardSimilarity extends java.io.Serializable{
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val starttime=System.nanoTime
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val ratingsFile = "data/MovieLens/ratings.dat"
    val logFile = "data/behavior.txt"

    import sqlContext.implicits._
    val ratingDF = sc.textFile(logFile, 2).cache().map{ line =>
      val fields = line.split(" ")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.toDF("userID","itemID","score")

//    val tmpRatings = sqlContext.read.text(ratingsFile).as[String]
//    val ratingDF = tmpRatings.map{line =>
//      val fields = line.split("::")
//      (fields(0).toInt,fields(1).toInt,fields(2).toInt)
//    }.toDF("userID", "itemID", "score")
//    val tmpRatings = sqlContext.read.text(logFile).as[String]
//    val ratingDF = tmpRatings.map{ line =>
//      val fields = line.split(" ")
//      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
//    }.toDF("userID","itemID","score")


    val cf = new JaccardSimilarity()
    val result = cf.simMatrix(ratingDF, (1, 50))
    result.foreach(s => println(s._1 + " " + s._2))
    val endtime = System.nanoTime
    println((endtime-starttime)*1.0/100000000)

    val result2 = cf.simItems(ratingDF, (1, 50))
    result2.foreach(s => println(s._1 + " " + s._2))
  }
}
