package SparkLearning

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class ItemBasedCF {

  def calc(): Unit ={
    println("hello")
  }

  def calcSimMatrix(ratingHistory: RDD[(String, Int)]) = {
    //得出每个item的用户列表
    val itemToUser = ratingHistory.map{
      case (userID, movieID) => (movieID, Set(userID))
    }.reduceByKey((a, b) => a.union(b))

    val t = itemToUser.collectAsMap()

    val cartesianResult = itemToUser cartesian itemToUser

    //println(t(3).intersect(t(2)).size.toDouble/t(3).union(t(2)).size)

    val itemSimMatrix = cartesianResult.map(s => ( (s._1._1, s._2._1),
      s._1._2.intersect(s._2._2).size.toDouble / s._1._2.union(s._2._2).size)
    )

    itemSimMatrix
  }


}
object ItemBasedCF{
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //环境
    val ratingFile = "data/MovieLens/ratings.dat"
    //val ratingFile = "data/behavior.txt"
    val moviesFile = "data/MovieLens/movies.dat"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //创建ibcf对象
    val ibcf = new ItemBasedCF()

    //评分历史记录:用户id，电影id，电影评分
    val ratingsHistory = sc.textFile(ratingFile, 2).cache().map { line =>
      var fields = line.split("::")
      (fields(0), fields(1).toInt, fields(2).toDouble)
    }

    //所有用户
    val userList = ratingsHistory.groupBy(t => t._1).map(t => t._1)

    //电影id和电影名称map
    val movies = sc.textFile(moviesFile, 2).cache().map { line =>
      var fields = line.split("::")
      (fields(0).toInt, fields(1) + "[" + fields(2) + "]")
    }
    val movieMap = movies.collectAsMap()

    //写入相似度计算结果
//    val startTime = System.nanoTime
//    val itemSimMatrix = ibcf.calcSimMatrix(ratingsHistory.map(s => (s._1, s._2)))
//    val endTime = System.nanoTime
//
//    val writer = new PrintWriter(new File("itemSimMatrix.txt"))
//    for(((id1, id2), sim) <- itemSimMatrix.toArray()){
//      writer.println(id1 + " " + id2 + " " + sim)
//    }

    //读取相似度结算结果
    val startTime = System.nanoTime
    val itemSimMatrix = sc.textFile("data/MovieLens/itemSimMatrix.txt", 2).cache().map{ line =>
      var fields = line.split(" ")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

    val k = 10
    val itemSimTopK = itemSimMatrix.map{ case(id1, id2, score) => (id1, Seq((id2, score)))}
      .reduceByKey((a, b) => a.union(b))
      .map{case (id,scores) => (id,scores.sortWith(_._2 > _._2).take(k)) }

    val writer = new PrintWriter(new File("itemSimTopK.txt"))
    for((id, seq) <- itemSimTopK){
      writer.println(id + " [" + seq.mkString(",") + "]")
    }
    writer.close()

    //打印运行时间
    val endTime = System.nanoTime
    println("run time: "+(endTime - startTime)*1.0/1000000000)

  }

}
