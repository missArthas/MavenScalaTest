package com.missArthas.mllib

package com.ximalaya.brain.mining.model.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{DataFrame, Row}


class MatrixCosin extends java.io.Serializable{

  /**
    * 计算所有item的相关item，取topK
    * @param data (userID, itemID)不重复
    * @param topK 每个item取相似的topK
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  //todo
  def simMatrixWithTopk(sc:SparkContext, data: RDD[(Long, Long, Double)], topK: Int = Int.MaxValue, itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue))  = {

    val itemSimMatrix = simMatrix(sc, data, itemNumStrict)

    val itemSimTopK = itemSimMatrix.map { case ((id1, id2), score) => (id1, Seq((id2, score))) }
      .reduceByKey((a, b) => a.union(b))
      .map { case (id, scores) => (id, scores.sortWith(_._2 > _._2).take(topK)) }

    itemSimTopK
  }

  /**
    * 计算所有item的相关item，输入的RDD形式
    * @param data (userID, itemID)不重复
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  //todo
  def simMatrix(sc:SparkContext, data: RDD[(Long, Long, Double)], itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue))  :RDD[((Long, Long), Double)] = {

    //item列表建立索引号
    val itemToIndex = data.map{
      case (userID, itemID, score) => itemID
    }.distinct().zipWithIndex().collectAsMap()

    val indexToItem = itemToIndex.map{
      case (item, index) => (index, item)
    }

    val itemMapLength = itemToIndex.size

    val itemToIndexBC = sc.broadcast(itemToIndex)
    val indexToItemBC = sc.broadcast(indexToItem)

    //每个用户的所有评分项
    val usersRatings = data.map{
      case (userID, itemID, score) => (userID, Seq((itemToIndexBC.value.getOrElse(itemID,0).toString.toInt, score)))
    }.reduceByKey((a, b) => a.union(b))
      .filter(f => f._2.size >= itemNumStrict._1 && f._2.size <= itemNumStrict._2)


    val vectorRDD = usersRatings.map{
      case (userID, itemSets) =>
        val vec: linalg.Vector = Vectors.sparse(itemMapLength, itemSets)
        vec
    }
    val matA_sim: RowMatrix = new RowMatrix(vectorRDD)

//    println("matA_sim")
//    matA_sim.rows.foreach(println)

    val result = matA_sim.columnSimilarities().entries.map{
      case MatrixEntry(index1, index2, score) =>
        val item1 = indexToItemBC.value.getOrElse(index1, 0).asInstanceOf[Long]
        val item2 = indexToItemBC.value.getOrElse(index2, 0).asInstanceOf[Long]
        (((item1, item2), score), ((item2, item1), score))
    }.flatMap { case (a, b) =>
      val res = List(a, b)
      res
    }

    result
  }

  /**
    * 计算所有item的相关item，输入的RDD形式
    * @param data (userID, itemID)不重复
    * @param itemNumStrict(a,b) a是最少，b是最大
    * @return
    */
  def simMatrixJaccard(sc:SparkContext, data: RDD[(Long, Long, Double)], itemNumStrict:(Int, Int) = (Int.MinValue, Int.MaxValue))  :RDD[((Long, Long), Double)] = {
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
      s._1._2.intersect(s._2._2).size.toDouble / s._1._2.union(s._2._2).size)
    ).filter(f => f._1._1 !=f._1._2)

    itemSimMatrix
  }

}

object MatrixCosin{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val album = "data/scoreMatrix.txt"

    val scoreMatrix: RDD[(Long, Long, Double)] = sc.textFile(album, 2).cache().map { line =>
      var fields = line.split(",")
      (fields(0).toLong, fields(1).toLong, fields(2).toDouble)
    }
    val cf = new MatrixCosin

    val result1 = cf.simMatrixJaccard(sc, scoreMatrix)
    val result2 = cf.simMatrix(sc, scoreMatrix)
    val result3 = cf.simMatrixWithTopk(sc, scoreMatrix, 2)



    println("Jaccard相似度结果")
    result1.foreach(println)

    println("余弦相似度结果")
    result2.foreach(println)
    println("take2")
    result2.take(2).foreach(println)

    println("余弦相似度top 2")

    result3.foreach(println)
  }
}



