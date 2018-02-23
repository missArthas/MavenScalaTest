package com.missArthas.mllib

import java.util.Comparator
import scala.math._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import java.text.DecimalFormat

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class ItemBasedTwoWayCF extends java.io.Serializable {
  /**
    * 计算item的相关item,按相似度排序,取前20名
    * @param sc
    * @param orders
    * @param range
    * @return
    */
  def itemSimilarity(sc: SparkContext, orders: RDD[(Long, Long, Long)], range:(Int, Int)=(1, 50), topK:Int = Int.MaxValue) = {
    val (itemPairFrequencySim, itemFrequency) = itemSimilarityAll(sc, orders, range)
    itemPairFrequencySim
      .map{ case (itemA, itemB, similarity) => (itemA, Seq((itemB, similarity)))}
      .reduceByKey((s, t) => s.union(t))
      .map{case (id, scores) => (id, scores.sortWith(_._2 > _._2).take(topK))}
  }

  /**
    * 计算item的相关item
    * @param sc
    * @param orders
    * @param range
    * @return
    */
  def itemSimilarityAll(sc: SparkContext, orders: RDD[(Long, Long, Long)], range:(Int, Int)=(1, 50)) = {
    //用户购买过的item集合
    //用户1：（item1,时间1),(item2,时间2)
    val userItems = orders
      .map { case (userID, itemID, time) =>
        (userID, Set((itemID, time)))
      }.reduceByKey((s, t) => s.union(t))
      .filter(f => f._2.size > range._1 && f._2.size <= range._2)

    //统计每个item被购买出现的次数
    //item1:3次
    val itemFrequency = userItems.flatMap { case (userID, set) => set.map(t => (t._1, 1.0)) }.reduceByKey((s, t) => s.+(t))
    val itemFrequencyBC = sc.broadcast(itemFrequency.collectAsMap())

    println("catFrequency:" + itemFrequencyBC.value.size)

    //生成item两两组合的子集
    val itemPairFrequency = userItems
      .flatMap { case (userID, set) =>
        val list = set.toList
        //子集配对
        val combination = list.combinations(2).map {
          t => if(t(0)._2 - t(1)._2 <= (0)) ((t(0)._1,t(1)._1),1.0) else ((t(0)._1,t(1)._1),0.0)
        }
        //子集配对swap专辑Id
        val combinationReverse = list.reverse.combinations(2).map {
          t => if(t(0)._2 - t(1)._2 <= (0)) ((t(0)._1,t(1)._1),1.0) else ((t(0)._1,t(1)._1),0.0) }
        val res = combination.++(combinationReverse)
        res
      }
    //统计专辑配对出现次数
    val itemPairFrequencyUnionFrequency = itemPairFrequency.reduceByKey((s, t) => s.+(t))

    //计算jaccard系数
    val itemPairFrequencySim = itemPairFrequencyUnionFrequency.map { case ((itemA, itemB), itemAIntersectB) =>
      val itemAUserNum = itemFrequencyBC.value.getOrElse(itemA, 0.0)
      val itemBUserNum = itemFrequencyBC.value.getOrElse(itemB, 0.0)
      val sim = itemAIntersectB / (sqrt(itemAUserNum * 1.0 * itemBUserNum)+1000)
      (itemA, itemB, sim)
    }
    (itemPairFrequencySim,itemFrequency)
  }
}

object ItemBasedTwoWayCF extends java.io.Serializable{
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val starttime=System.nanoTime
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val data = "data/behaviorWithTime.txt"

    import sqlContext.implicits._
    val ratingDF = sc.textFile(data, 2).cache().map{ line =>
      val fields = line.split(" ")
      (fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
      //.toDF("userID","itemID","time")


    val cf = new ItemBasedTwoWayCF()

    val itemList = List(1, 2).toDF("userID")
    val result3 = cf.itemSimilarity(sc, ratingDF).toDF("itemId", "simList")

    println("排序计算结果")
    val user1 = result3.where("itemID = '1'")

    user1.rdd.foreach{case Row(itemID,list)=>println(list)}
    //result3.foreach(s => println(s._1 + " " + s._2.toSeq))

    println((1464666964 - 1514796030)/(3600*24))

  }
}

