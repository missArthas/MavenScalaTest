package com.missArthas.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shhuang on 2017/3/9.
  */
object SparkKmeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //训练数据
    val data = sc.textFile("data/kmeans/kmeans_testSet.txt").cache()
    val parsedData = data.map(line => Vectors.dense(line.split(' ').map(_.toDouble)))

    // 将数据集聚类，2个类，20次迭代，进行模型训练形成数据模型
    val numClusters = 4
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)

    // 打印数据模型的中心点
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    // 使用误差平方之和来评估数据模型
    val cost = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + cost)

    // 使用模型测试单点数据
//    println("Vectors 0.2 0.2 0.2 is belongs to clusters:" + model.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
//    println("Vectors 0.25 0.25 0.25 is belongs to clusters:" + model.predict(Vectors.dense("0.25 0.25 0.25".split(' ').map(_.toDouble))))
//    println("Vectors 8 8 8 is belongs to clusters:" + model.predict(Vectors.dense("8 8 8".split(' ').map(_.toDouble))))

    // 交叉评估1，只返回结果
    val testdata = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val result1 = model.predict(testdata)
    //result1.saveAsTextFile("data/kmeans/result_kmeans1")
    println("predict result1:"+result1.toString())

    // 交叉评估2，返回数据集和结果
    val result2 = data.map {
      line =>
        val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
        val prediction = model.predict(linevectore)
        line + " " + prediction
    }
    println("predict result2:"+result2.toString())

    sc.stop()

  }

}
