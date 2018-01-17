package com.missArthas.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.rdd.RDD

object VectorTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val album = "data/album_sim.txt"

    // Create a dense vector (1.0, 0.0, 3.0).
    val dv: linalg.Vector = Vectors.dense(1.0, 0.0, 3.0)
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values  corresponding to nonzero entries.
    val sv1: linalg.Vector = Vectors.sparse(5, Array(0, 1), Array(1.0, 3.0))
    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
    val tmp1: linalg.Vector = Vectors.sparse(100, Seq((1, 1.0), (0, 1.0)))
    val tmp2: linalg.Vector = Vectors.sparse(100, Seq((0, 1.0), (1, 1.0), (2, 1.0)))
    val tmp3: linalg.Vector = Vectors.sparse(100, Seq((0, 1.0), (1, 1.0), (2, 2.0), (3, 1.0)))

    val v1 = new IndexedRow(1, tmp1)
    val v2 = new IndexedRow(2, tmp2)
    val v3 = new IndexedRow(3, tmp3)

    val vRDD = sc.parallelize(Array(tmp2,tmp3,tmp1))



    val Features1: RDD[(Int, Array[Double])] = sc.textFile(album, 2).cache().map { line =>
      var fields = line.split(",")
      (fields(0).toInt, Array(fields(1).toDouble, fields(2).toDouble, fields(3).toDouble))
    }

    val rowsA_sim = Features1.map { s =>
      IndexedRow(s._1.toLong, org.apache.spark.mllib.linalg.Vectors.dense(s._2))
    }
    println("vRDD")
    vRDD.foreach(println)
    val matA_sim: RowMatrix = new RowMatrix(vRDD)
    println("matA_sim")
    matA_sim.rows.foreach(println)

    val result = matA_sim.columnSimilarities()
    println("result")
    println(result.numCols())
    println(result.numRows())
    println()
    result.entries.foreach(println)



//    val res = sc.parallelize(Array(1.0, 0.0, 3.0, 1.0, 0.0, 3.0, 1.0, 0.0, 3.0))       //转换为RDD[Array[String]]类型
//        .map(_.toDouble)                           //转换为RDD[Array[Double]]类型
//      .map(line => Vectors.dense(line))               //转换为RDD[Vector]类型
//      .map((Vc) => new IndexedRow(3,Vc))     //转换为RDD[IndexedRow]类型
//
//    res.foreach(print)
//    val mat = new IndexedRowMatrix(res,3,3)
//    mat.rows.foreach(println)
//    val result = mat.columnSimilarities()
//
//    println(mat.numRows())
//    println(mat.numCols())
//    println(mat.rows.foreach(println))
//
//    val m = mat.numRows()
//    val n = mat.numCols()

  }

}
