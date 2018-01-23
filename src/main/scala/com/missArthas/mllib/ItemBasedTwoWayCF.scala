package com.missArthas.mllib

import java.util.Comparator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.rdd.RDD
import java.sql.Timestamp
import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


class ItemBasedTwoWayCF extends java.io.Serializable {

//  def orderingTopKForCf() = {
//    com.google.common.collect.Ordering.from(new Comparator[(Int,Double)] {
//          override def compare(o1: (Int,Double), o2: (Int,Double)): Int = {
//            Doubles.compare(o1._2, o2._2)
//          }
//    })
//  }
//
//
  def simLine(sim: RDD[(Long, Long, Double)]) = {
    val albumSimLine = sim.map{case (catA,catB,sim) => (catA,Seq((catB,sim)))}.reduceByKey((s,t) => s.union(t))
      .map { case (id, scores) => (id, scores.sortWith(_._2 > _._2).take(20)) }
    albumSimLine
  }
  
  def albumSimTriColumn( cutNumSim: RDD[(Long, Array[(Long, Double)])]) = {
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
object ItemBasedTwoWayCF{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val album = "data/behaviorWithTime.txt"

    val scoreMatrix: RDD[(Long, Long, Long)] = sc.textFile(album, 2).cache().map { line =>
      var fields = line.split(",")
      (fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
    val cf = new ItemBasedTwoWayCF

    import java.text.SimpleDateFormat
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date1 = sdf.parse("2011-02-04")
    val date2 = sdf.parse("2011-01-06")
    println((date2.getTime-date1.getTime) / (1000*3600*24))
    println((1452741307 - 1463985631)/(3600*24))

    //val result1 = cf.albumSim(sc, scoreMatrix)

    //result1.take(10).foreach(println)

  }
}


