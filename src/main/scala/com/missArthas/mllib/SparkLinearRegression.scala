package com.missArthas.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
/**
  * Created by shhuang on 2017/3/9.
  */
object SparkLinearRegression {

    // 屏蔽不必要的日志显示终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    def main(args:Array[String]): Unit ={
      // 设置运行环境
      val conf = new SparkConf().setAppName("linear regression").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)



      val training = sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )).toDF("label", "features")

      //创建一个LogisticRegression实例，这是一个Estimator.
      val lr = new LogisticRegression()
      //打印参数
      //println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

      //调用实例的set方法设置参数
      lr.setMaxIter(10)
        .setRegParam(0.01)

      // 学习LogisticRegression模型，model1是一个transformer
      val model1 = lr.fit(training)

      println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)
      println(model1.coefficients)

      // 通过paramap来设置参数
      val paramMap = ParamMap(lr.maxIter -> 20)
        .put(lr.maxIter, 30)
        .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

      // 两个ParamMap之间可以相加合并.
      val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability") // Change output column name
      val paramMapCombined = paramMap ++ paramMap2

      val model2 = lr.fit(training, paramMapCombined)
      println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

      //测试数据
      val test = sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
        (0.0, Vectors.dense(3.0, 2.0, -0.1)),
        (1.0, Vectors.dense(0.0, 2.2, -1.5))
      )).toDF("label", "features")

      //model2的transform()会只选择features的数据，不会把label数据包含进去
      model2.transform(test)
        .select("features", "label", "myProbability", "prediction")
        .collect()
        .foreach { case Row(features, label, prob, prediction) =>
          println(s"($features, $label) -> prob=$prob, prediction=$prediction")
        }
    }
}
