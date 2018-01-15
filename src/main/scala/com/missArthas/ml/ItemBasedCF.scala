package com.missArthas.ml

import java.util.Comparator

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.google.common.primitives.Doubles
import java.text.DecimalFormat

import org.apache.log4j.{Level, Logger}



class ItemBasedCF extends java.io.Serializable {

  def orderingTopKForCf() = {
    com.google.common.collect.Ordering.from(new Comparator[(Int,Double)] {
      override def compare(o1: (Int,Double), o2: (Int,Double)): Int = {
        Doubles.compare(o1._2, o2._2)
      }
    })
  }


  def simLine(sim: RDD[(Int, Int, Double)]) = {
    val albumSimLine = sim.map{case (catA,catB,sim) => (catA,Seq((catB,sim)))}.reduceByKey((s,t) => s.union(t)).map{case (albumId,seq) =>
      val top = orderingTopKForCf.greatestOf(seq, 20).toArray(Array.empty[(Int,Double)])
      (albumId,top)
    }
    albumSimLine
  }

  def albumSimTriColumn( cutNumSim: RDD[(Int, Array[(Int, Double)])]) = {
    cutNumSim.flatMap{case (albumIdA,relatedAlbum) =>
      relatedAlbum.map{case (albumIdB,score) => s"$albumIdA\t$albumIdB\t$score"}
    }
  }

  /**
    * 计算专辑的相关专辑,按相似度排序,取前20名
    * @param sc
    * @param usrAlbumScore
    * @param usrListenNumLower
    * @param usrListenNumUpper
    * @return
    */
  def albumSim(sc: SparkContext, usrAlbumScore: RDD[(String, Int)], usrListenNumLower: Int = 1, usrListenNumUpper: Int = 50) = {

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
  def albumSimAll(sc: SparkContext, usrAlbumScore: RDD[(String, Int)], usrListenNumLower: Int = 1, usrListenNumUpper: Int = 50) = {
    //用户收听专辑的集合
    val usrAlbums = usrAlbumScore
      .map { case (usrId, cat) =>
        (usrId, Set(cat))
      }.reduceByKey((s, t) => s.union(t))
      .filter(f => f._2.size > usrListenNumLower && f._2.size <= usrListenNumUpper)

    println("ussAlbums: ")
    usrAlbums.foreach(println)

    //统计每个专辑出现的次数
    val catFrequency = usrAlbums.flatMap { case (usrId, arr) => arr.map(t => (t, 1.0)) }.reduceByKey((s, t) => s.+(t))

    println("catFrequency: ")
    catFrequency.foreach(println)

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
    println("catPairFrequency: ")
    catPairFrequency.foreach(println)


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


  //根据用户收听历史，查询这些专辑的相关专辑，然后计算用户对这些相关专辑的喜好程度，做推荐（协同过滤）
  def cfRecommend(sc:SparkContext,userHistory: RDD[(String, Array[(Int, Double)])], cutNumSim: RDD[(Int, Array[(Int, Double)])]): RDD[(String, Array[((Int, Double), (Int, Double))])] = {
    val cutNumSimMapBroadCast = sc.broadcast(cutNumSim.collectAsMap)
    val finalResultReason = userHistory.flatMap {
      case (usrId, usrHisList) =>
        val usrHisIds = usrHisList.map(s => s._1)
        val relatedAlbum = usrHisList.map {
          case (hisId, hisRate) =>
            val simAlbum = cutNumSimMapBroadCast.value.getOrElse(hisId, Array((0, 0.0)))
            (usrId, hisId, simAlbum, hisRate)
        }.filter { case (usrId, hisId, simAlbum, hisRate) => simAlbum(0)._1 != 0 }

        val reasonMap = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => (simAlbumId, hisId, simAlbumCor) }
        }.groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).sortWith((l, m) => l._2 > m._2).apply(0)))

        val finalResult = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => ((usrId, simAlbumId), hisRate * simAlbumCor) }
        }.groupBy(f => f._1).map(t => (t._1, t._2.map(s => s._2).sum))
          .map(s => (s._1._1, s._1._2, s._2))
          .groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).toArray.filter(t => !usrHisIds.contains(t._1)).sortWith((l, m) => l._2 > m._2).take(80)))
          .map(fr => (fr._1, fr._2.map(t => (t, reasonMap.get(t._1).get))))
        finalResult
    }
    finalResultReason
  }

  //TODO change albumId from int to long
  def refactCfRecommend(sc:SparkContext,userHistory: RDD[(String, Array[(Int, Double)])], cutNumSim: Array[(Int, Array[(Int, Double)])]) = {
    val cutNumSimMapBroadCast = sc.broadcast(cutNumSim.toMap)
    val finalRecommendResult = userHistory.flatMap {
      case (usrId, usrHisList) =>
        val usrHisIds = usrHisList.map(s => s._1).toSet
        val relatedAlbum = usrHisList.map {
          case (hisId, hisRate) =>
            val simAlbum = cutNumSimMapBroadCast.value.getOrElse(hisId, Array((0, 0.0)))
            (usrId, hisId, simAlbum, hisRate)
        }.filter { case (usrId, hisId, simAlbum, hisRate) => simAlbum(0)._1 != 0 }

        val reasonMap = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => (simAlbumId, hisId, simAlbumCor) }
        }.groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).sortWith((l, m) => l._2 > m._2).apply(0)))

        val finalResult = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => ((usrId, simAlbumId), hisRate * simAlbumCor) }
        }.groupBy(f => f._1).map(t => (t._1, t._2.map(s => s._2).sum))
          .map(s => (s._1._1, s._1._2, s._2))
          .groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).toArray.filter(t => !usrHisIds.contains(t._1)).sortWith((l, m) => l._2 > m._2).take(80)))
          .map(fr => (fr._1, fr._2.map(t => (t, reasonMap.get(t._1).get))))
        finalResult
    }.map { t => (t._1.toLong, t._2.map(s => (s._1._1.toLong, new DecimalFormat("#.00000000").format(s._1._2).toDouble, s._2._1.toLong)))} // + ":" + s._2._2
    finalRecommendResult
  }

}

object ItemBasedCF extends java.io.Serializable {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val ibcf = new ItemBasedCF()
    val logFile = "data/behavior.txt"

    val ratingDF = sc.textFile(logFile, 2).cache().map{ line =>
      val fields = line.split(" ")
      (fields(0).toString, fields(1).toInt)
    }

    ibcf.albumSim(sc, ratingDF)
    ratingDF.foreach(println)

  }
}
