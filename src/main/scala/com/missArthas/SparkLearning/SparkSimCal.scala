package com.missArthas.SparkLearning

import java.io.{File, PrintWriter}
import java.text.DecimalFormat
import java.util.Comparator

import com.google.common.primitives.Doubles
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkSimCal extends java.io.Serializable{
  Logger.getLogger("org").setLevel(Level.ERROR)

  def orderingTopKForCf() = {
    com.google.common.collect.Ordering.from(new Comparator[(String,Double)] {
      override def compare(o1: (String,Double), o2: (String,Double)): Int = {
        Doubles.compare(o1._2, o2._2)
      }
    })
  }


  def simLine(sim: RDD[(Int, Int, Double)], k:Int) = {
//    val albumSimLine = sim.map{case (catA,catB,sim) => (catA,Seq((catB,sim)))}.reduceByKey((s,t) => s.union(t)).map{case (albumId,seq) =>
//      val top = orderingTopKForCf.greatestOf(seq, 20).toArray(Array.empty[(Int,Double)])
//      (albumId,top)
//    }
//    albumSimLine
    //sim.foreach(println)
    val albumSimLine = sim.map{case (catA,catB,sim) => (catA,Seq((catB,sim)))}.reduceByKey((s,t) => s.union(t))
    //albumSimLine.foreach(println)
    val sortResult = albumSimLine.map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(k)) }
    //sortResult.foreach(println)
    sortResult
  }

  def albumSim(sc: SparkContext, usrAlbumScore:RDD[(String, Int)]) = {
    val (catPairFrequencySim,catFrequency) = albumSimAll(sc, usrAlbumScore:RDD[(String, Int)])
    simLine(catPairFrequencySim,2)

  }

  def albumSimAll(sc: SparkContext, usrAlbumScore:RDD[(String, Int)])={
    /*
    val logFile = "data/behavior.txt" // Should be some file on your server.
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(logFile, 2).cache()

    val usrAlbumScore = srcData.map{ line =>
      val fields = line.split(" ")
      (fields(0), fields(1))
    }
    */
    val usrAlbums = usrAlbumScore
      .map { case (usrId, cat) =>
        (usrId, Set(cat))
      }.reduceByKey((s, t) => s.union(t))


    //val unionData = srcData.map(line => (line.toLowerCase(), Set(1))).reduceByKey((a,b) => a.union(b))
    //usrAlbumScore.foreach(print)
    val catFrequency = usrAlbums.flatMap { case (usrId, arr) => arr.map(t => (t, 1.0)) }.reduceByKey((s, t) => s.+(t))

    val catFrequencyBC = sc.broadcast(catFrequency.collectAsMap())
//    println("BC:")
//    catFrequency.foreach(println)
//    println()
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

    println("计算jaccard系数")
    //计算jaccard系数
    val catPairFrequencySim = catPairFrequencyUnionFrequency.map { case ((catA, catB), catAAndBUsrNum) =>
      //println("catA:",catA,"catB",catB)
      val catAUsrNum = catFrequencyBC.value.getOrElse(catA, 0.0)
      val catBUsrNum = catFrequencyBC.value.getOrElse(catB, 0.0)
      val sim = catAAndBUsrNum / (catAUsrNum + catBUsrNum - catAAndBUsrNum + 100)
      (catA, catB, sim)
    }

    //catPairFrequencyUnionFrequency.foreach(println)
    //usrAlbums.foreach(println)
//    println("物品之间相似度矩阵")
//    catPairFrequencySim.foreach(println)
//    println()

    (catPairFrequencySim,catFrequency)
  }

  //根据用户收听历史，查询这些专辑的相关专辑，然后计算用户对这些相关专辑的喜好程度，做推荐（协同过滤）
  def cfRecommend(sc:SparkContext,userHistory: RDD[(String, Array[(Int, Double)])], cutNumSim: RDD[(Int, Array[(Int, Double)])]): RDD[(String, Array[((Int, Double), (Int, Double))])] = {

    //val cutNumSimMapBroadCast = sc.broadcast(cutNumSim.collectAsMap)
      val cutNumSimMapBroadCast = cutNumSim.collectAsMap()
//    println("userHistory:")
//    userHistory.foreach(s => println(s._1, s._2.mkString(",")))
//    println()
//
//    println("cutNumSimMapBroadCast")
//    cutNumSimMapBroadCast.value.map(s => (s._1, s._2(0), s._2(1))).foreach(println)
//    println()
    println("cfRecommend function")
    val finalResultReason = userHistory.flatMap {

      case (usrId, usrHisList) =>
        println("usrId:",usrId)
        val usrHisIds = usrHisList.map(s => s._1)
        val relatedAlbum = usrHisList.map {
          case (hisId, hisRate) =>
            val simAlbum = cutNumSimMapBroadCast.getOrElse(hisId, Array((0, 0.0)))
            (usrId, hisId, simAlbum, hisRate)
        }.filter { case (usrId, hisId, simAlbum, hisRate) => simAlbum(0)._1 != 0 }

//        println("relatedAlbum:")
//        relatedAlbum.foreach(s => println(s._1,s._2,s._3.mkString(","),s._4))
//        println()

        //根据simAlbumCor排序
        val reasonMap = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => (simAlbumId, hisId, simAlbumCor) }
        }.groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).sortWith((l, m) => l._2 > m._2).apply(0)))

//        println("reasonMap:")
//        reasonMap.foreach(println)
//        println()

        val finalResultStep1 = relatedAlbum.flatMap {
          case (usrId, hisId, simAlbum, hisRate) =>
            simAlbum.map { case (simAlbumId, simAlbumCor) => ((usrId, simAlbumId), hisRate * simAlbumCor) }
        }

//        println("finalResultStep1:")
//        finalResultStep1.foreach(println)
//        println()

        val finalResult = finalResultStep1.groupBy(f => f._1).map(t => (t._1, t._2.map(s => s._2).sum))
          .map(s => (s._1._1, s._1._2, s._2))
          .groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).toArray.filter(t => !usrHisIds.contains(t._1)).sortWith((l, m) => l._2 > m._2).take(80)))
          .map(fr => (fr._1, fr._2.map(t => (t, reasonMap.get(t._1).get))))


        finalResult
    }
    finalResultReason
  }

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
          .groupBy(f => f._1).map(t => (t._1, t._2.map(s => (s._2, s._3)).toArray.filter(t => !usrHisIds.contains(t._1)).sortWith((l, m) => l._2 > m._2).take(10)))
          .map(fr => (fr._1, fr._2.map(t => (t, reasonMap.get(t._1).get))))
        finalResult
    }.map { t => (t._1.toLong, t._2.map(s => (s._1._1.toLong, new DecimalFormat("#.00000000").format(s._1._2).toDouble, s._2._1.toLong)))} // + ":" + s._2._2
    finalRecommendResult
  }


  def run(sc:SparkContext,usrAlbumScore:RDD[(String, Int, Double)]) = {
    //val usrAlbumScore = new ReadFile().readWalsScore(sc, usrAlbumScorePath, 200)
    val albumRelation =  albumSim(sc,usrAlbumScore.map(t => (t._1,t._2))).map(s => (s._1,(s._2.toArray)))

//    println("userAlbumScore:")
//    usrAlbumScore.foreach(println)
//    println()
//
//    println("albumRelation:")
//    albumRelation.map(s => (s._1, s._2.mkString(","))).foreach(println)
//    println()

    val userHistory = usrAlbumScore.map(s => (s._1,Array((s._2,s._3)))).reduceByKey((s, t) => s.union(t))

    val recommend = cfRecommend(sc, userHistory, albumRelation)
      .map { t => t._1 + "\t" + "[" + t._2.map(s => s._1._1 + ":" + new DecimalFormat("#.00000000").format(s._1._2).toDouble + ":" + s._2._1).mkString(",") + "]" } // + ":" + s._2._2

    val albumRelationLineToString = albumRelation.map{case (albumId,relation) => albumId+"\t"+relation.map(t => t._1 + ":" + t._2).mkString("[",",","]")}
    (albumRelationLineToString,recommend)
    //    albumRelationLineToString
  }



  def main(args: Array[String]): Unit = {
    val logFile = "data/behavior.txt" // Should be some file on your server.
    val movielens = "data/MovieLens/ratings.dat"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(logFile, 2).cache()
    val ratingsFile = sc.textFile(movielens, 2).cache()

    val usrAlbumScore = srcData.map { line =>
      val fields = line.split(" ")
      (fields(0), fields(1).toInt)
    }

    val ratings = ratingsFile.map { line =>
      var fields = line.split("::")
      (fields(0), fields(1).toInt, fields(2).toDouble)
    }









    //计算Jaccard
    //算出item之间的相似度，结果是相似度矩阵
    //最后结果是s(j,k)，与j最相似的k个（物品id，相似度）
    //    val result = albumSim(sc, usrAlbumScore)
    //    println("after calculation and resort:")
    //    result.foreach(println)

    //run
//     val rec = run(sc,srcData.map{line =>
//       val fields = line.split(" ")
//       (fields(0),fields(1).toInt,fields(2).toDouble)
//     })


    val starttime=System.nanoTime
    val rec = run(sc, ratings)
    val endtime = System.nanoTime()
    val seconds = endtime - starttime

    println("run result:")
    val writer = new PrintWriter(new File("learningScala.txt"))
    for(r:String <- rec._2){
      writer.println(r)
    }
    writer.println("run time:"+seconds/1000000000)
      //rec._2.foreach(writer.println)
    writer.close()
     //rec._1.foreach(println)
     //rec._2.foreach(println)
  }

}

