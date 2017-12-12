package com.missArthas.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shhuang on 2017/3/9.
  */
object SparkLinearRegression {
    def main(args:Array[String]): Unit ={
      // 屏蔽不必要的日志显示终端上
      Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      // 设置运行环境
      val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
      val sc = new SparkContext(conf)

      // Load and parse the data
      val data = sc.textFile("data/LinearRegression/testdata.txt")
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }

      // Building the model
      val numIterations = 100
      val model = LinearRegressionWithSGD.train(parsedData, numIterations)

      // Evaluate model on training examples and compute training error
      val valuesAndPreds = parsedData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      println("weight:"+model.weights)
      val v1=Vectors.dense(1.0)
      //println("predict:"+model.predict(v1))

      val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce (_ + _) / valuesAndPreds.count
      println("training Mean Squared Error = " + MSE)


      sc.stop()
    }
}
