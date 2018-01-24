package com.missArthas.mllib

import java.util.Comparator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import java.text.DecimalFormat

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class ItemBasedTwoWayCF extends java.io.Serializable {


  def simLine(sim: RDD[(Long, Long, Double)]) = {
    val albumSimLine = sim.map{case (catA,catB,sim) => (catA,Seq((catB,sim)))}.reduceByKey((s,t) => s.union(t))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(20)) }
    albumSimLine
  }


  /**
    * 计算专辑的相关专辑,按相似度排序,取前20名
    * @param sc
    * @param usrAlbumScore
    * @param usrListenNumLower
    * @param usrListenNumUpper
    * @return
    */
  def albumSim(sc: SparkContext, usrAlbumScore: RDD[(Long, Long, Long)], usrListenNumLower: Int = 1, usrListenNumUpper: Int = 50) = {

    val (catPairFrequencySim,catFrequency) = albumSimAll(sc,usrAlbumScore,usrListenNumLower,usrListenNumUpper)
    simLine(catPairFrequencySim)
  }

  /**
    * 计算专辑的相关专辑
    * @param sc
    * @param usrAlbumScore
    * @param usrListenNumLower
    * @param usrListenNumUpper
    * @return
    */
  def albumSimAll(sc: SparkContext, usrAlbumScore: RDD[(Long, Long, Long)], usrListenNumLower: Int = 1, usrListenNumUpper: Int = 50) = {
    //用户收听专辑的集合
    val usrAlbums = usrAlbumScore
      .map { case (usrId, cat, time) =>
        (usrId, Set(cat))
      }.reduceByKey((s, t) => s.union(t))
      .filter(f => f._2.size > usrListenNumLower && f._2.size <= usrListenNumUpper)

    //统计每个专辑出现的次数
    val catFrequency = usrAlbums.flatMap { case (usrId, arr) => arr.map(t => (t, 1.0)) }.reduceByKey((s, t) => s.+(t))

    val catFrequencyBC = sc.broadcast(catFrequency.collectAsMap())

    println("catFrequency:" + catFrequencyBC.value.size)

    //生成专辑两两组合的子集
    val catPairFrequency = usrAlbums
      .flatMap { case (usrId, arr) =>
        val list = arr.toList
        //子集配对
        val combination = list.combinations(2).map { t => ((t(0), t(1)), 1.0) }
        //子集配对swap专辑Id
        val combinationReverse = list.reverse.combinations(2).map { t => ((t(0), t(1)), 1.0) }
        val res = combination.++(combinationReverse)
        res
      }

    //统计专辑配对出现次数
    val catPairFrequencyUnionFrequency = catPairFrequency.reduceByKey((s, t) => s.+(t))

    //    println("catPairFrequencyUnionFrequency:"+catPairFrequencyUnionFrequency.count)

    //计算jaccard系数
    val catPairFrequencySim = catPairFrequencyUnionFrequency.map { case ((catA, catB), catAAndBUsrNum) =>
      val catAUsrNum = catFrequencyBC.value.getOrElse(catA, 0.0)
      val catBUsrNum = catFrequencyBC.value.getOrElse(catB, 0.0)
      val sim = catAAndBUsrNum / (catAUsrNum + catBUsrNum - catAAndBUsrNum + 100)
      (catA, catB, sim)
    }
    (catPairFrequencySim,catFrequency)
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
    val result3 = cf.albumSim(sc, ratingDF)

    println("排序计算结果")
    result3.foreach(s => println(s._1 + " " + s._2.toSeq))

  }

  /**
    * X->B,C,D
    *
    * @param result
    * @return X->B,C,D
    *         B->X
    *         C->X
    *         D->X
    */
  private def linkOldAlbum(result: RDD[(Long, Array[(Long, Float)])]): RDD[(Long, Array[(Long, Float)])]={

    val reverseItem = result.flatMap{
      case (itemID, arr) =>
        val list = arr.toList
        //子集配对
        val reverseItem = list.map { t => (t._1, Array((itemID, t._2))) }
        reverseItem
    }.reduceByKey((a, b) => a.union(b)).union(result).reduceByKey((a, b) => a.union(b))

    val finalResult = reverseItem.map{case (id,scores) => (id,scores.distinct.sortWith(_._2 > _._2)) }

    finalResult
  }

}

