package com.missArthas.mllib

import java.util.Comparator

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.google.common.primitives.Doubles
import java.text.DecimalFormat

import com.missArthas.SparkLearning.ItemBasedCF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}




class JaccardSimilarity extends java.io.Serializable{

  /**
    * 计算item的相关item
    * @param data (userID, itemID)不重复
    * @return
    */
  def simAll(data: DataFrame, itemNumStrict:(Int, Int)) = {
    //每个物品对应的对他有过记录的用户集
    val itemUserSet = data.rdd.map{
      case Row(userID, movieID, score) => (movieID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))
      .filter(f => f._2.size > itemNumStrict._1 && f._2.size <= itemNumStrict._2)

    //笛卡尔积,大量运算
    val cartesianResult = itemUserSet cartesian itemUserSet

    //(s._1._1, s._2._1)是物品id对
    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toDouble / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    itemSimMatrix
  }

  def simAll(data: DataFrame, itemNumStrict:(Int, Int), topk: Int) = {
    //每个物品对应的对他有过记录的用户集
    val itemUserSet = data.rdd.map{
      case Row(userID, movieID, score) => (movieID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))
      .filter(f => f._2.size > itemNumStrict._1 && f._2.size <= itemNumStrict._2)

    //笛卡尔积,大量运算
    val cartesianResult = itemUserSet cartesian itemUserSet

    //(s._1._1, s._2._1)是物品id对
    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toDouble / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    //对每个物品的相似物品列表排序，取topk
    val itemSimTopK = itemSimMatrix.map{ case((id1, id2), score) => (id1, Seq((id2, score)))}
      .reduceByKey((a, b) => a.union(b))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(topk)) }

    itemSimTopK
  }

  def simAll(data: RDD[(Int, Int, Double)], itemNumStrict:(Int, Int)) = {
    val itemToUser = data.map{
      case (userID, movieID, score) => (movieID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))

    val t = itemToUser.collectAsMap()

    val cartesianResult = itemToUser cartesian itemToUser

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
    val result = cf.simAll(ratingDF, (1, 50))
    val endtime = System.nanoTime
    println((endtime-starttime)*1.0/100000000)

    val result2 = cf.simAll(ratingDF, (1, 50), 2)
    result2.foreach(s => println(s._1 + " " + s._2))
  }
}
