package com.missArthas.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel


class JaccardSimilarity extends java.io.Serializable{

  /**
    * 计算所有item的相关item，输入是DataFrame形式
    * @param data (userID, itemID)不重复
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  def simMatrix(data: DataFrame, itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue)) :RDD[((Long, Long), Float)] = {
    //DataFrame转化成RDD
    val dataRDD = data.rdd.map{case Row(userID: Long, itemID: Long, score: Float) => (userID, itemID, score)}

    //jaccard相似度，交集／并集
    val itemSimMatrix = simMatrix(dataRDD, itemNumStrict)

    itemSimMatrix
  }

  /**
    * 计算所有item的相关item，取topK
    * @param data (userID, itemID)不重复
    * @param topK 每个item取相似的topK
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  def simMatrixWithTopk(data: DataFrame, topK: Int = Int.MaxValue, itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue)) = {
    //笛卡尔积之后，item之间的相似度
    val itemSimMatrix = simMatrix(data, itemNumStrict)

    //给出每种item的最相似的topk
    val itemSimTopK = itemSimMatrix.map{ case((id1, id2), score) => (id1, Seq((id2, score)))}
      .reduceByKey((a, b) => a.union(b))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(topK)) }

    itemSimTopK
  }

  /**
    * 计算itemList里item的相关item，取topK
    * @param itemList 要计算的目标item集合，可以是1个，可以是多个
    * @param data (userID, itemID)不重复
    * @param topK 每个item取相似的topK
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  def itemsRelated(sc: SparkContext,itemList: DataFrame, data: DataFrame, topK: Int, itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue)) = {
    //对item有过记录的user集合
    //选出itemList
    val allItemToUser = data.rdd.map{
      case Row(userID, itemID, score) => (itemID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))
      .filter(f => f._2.size >= itemNumStrict._1 && f._2.size <= itemNumStrict._2)
    println("所有item数量：", allItemToUser.count())
    itemList.rdd.foreach(println)

    //广播映射表表
    val choosedItemMap = itemList.rdd.map{
      case Row(itemID) => (itemID, true)
    }
    println("需要计算的item数量:", choosedItemMap.count())

    val choosedItemMapBC = sc.broadcast(choosedItemMap.collectAsMap())
    val choosedItemToUser = allItemToUser.filter(item => choosedItemMapBC.value.getOrElse(item._1, false))
    println("目标items经过映射筛选之后数量:", choosedItemToUser.count())
    println("测试map 的key，value类型")
    println("user id string: 11676655:", choosedItemMapBC.value.getOrElse("11676655", false))
    println("user id int: 11759520:", choosedItemMapBC.value.getOrElse(11759520,false))

    //笛卡尔积,大量运算
    val cartesianResult = choosedItemToUser cartesian allItemToUser
    println("笛卡尔积结果size：", cartesianResult.count())

    //jaccard相似度，交集／并集
    //(s._1._1, s._2._1)是物品id对
    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toFloat / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    //给出每种item的最相似的topk
    val itemSimTopK = itemSimMatrix.map{ case((id1: Long, id2: Long), score: Float) => (id1, Array((id2, score)))}
      .reduceByKey((a, b) => a.union(b))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(topK)) }
    itemSimTopK
  }

  /**
    * 计算所有item的相关item，输入的RDD形式
    * @param data (userID, itemID)不重复
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  private def simMatrix(data: RDD[(Long, Long, Float)], itemNumStrict:(Int, Int))  :RDD[((Long, Long), Float)] = {
    //对item有过记录的user集合
    val itemToUser = data.map{
      case (userID, itemID, score) => (itemID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))
      .filter(f => f._2.size >= itemNumStrict._1 && f._2.size <= itemNumStrict._2)

    //笛卡尔积,大量运算
    val cartesianResult = itemToUser cartesian itemToUser

    //jaccard相似度，交集／并集
    //(s._1._1, s._2._1)是物品id对
    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toFloat / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    itemSimMatrix
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
      (fields(0).toLong, fields(1).toLong, fields(2).toFloat)
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
    //    val result = cf.simMatrix(ratingDF, (1, 50))
    //    result.foreach(s => println(s._1 + " " + s._2))
    //    val endtime = System.nanoTime
    //    println("time is : "+(endtime-starttime)*1.0/1000000000+"'s")
    //
    //    val result2 = cf.simItems(ratingDF, 2)
    //    result2.foreach(s => println(s._1 + " " + s._2))

    val itemList = List(1, 2).toDF("userID")
    val result3 = cf.itemsRelated(sc, itemList, ratingDF, 2)
    println("排序计算结果")
    result3.foreach(s => println(s._1 + " " + s._2.toSeq))

    val result4 = linkOldAlbum(result3)
    println("最终结果")
    result4.foreach(s => println(s._1 + " " + s._2.toSeq))
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
