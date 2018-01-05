package com.missArthas.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.{LogisticRegression,LogisticRegressionModel}
import org.apache.spark.ml.{Pipeline,PipelineModel}


object CrossValidateIris {
  case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/iris.data")
      .map(_.split(","))
      .map(p => Iris(Vectors.dense(p(0).toDouble,p(1).toDouble,p(2).toDouble, p(3).toDouble), p(4).toString()))
      .toDF()

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(data)

    val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(50)
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val lrPipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))

    val paramGrid = new ParamGridBuilder().
      addGrid(lr.elasticNetParam, Array(0.2,0.8)).
      addGrid(lr.regParam, Array(0.01, 0.1, 0.5)).
      build()

    val cv = new CrossValidator().
      setEstimator(lrPipeline).
      setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")).
      setEstimatorParamMaps(paramGrid).
      setNumFolds(3) // Use 3+ in practice

    val cvModel = cv.fit(trainingData)
    val lrPredictions=cvModel.transform(testData)

    lrPredictions.select("predictedLabel", "label", "features", "probability").
      collect().
      foreach{
        case Row(predictedLabel: String, label:String,features:Vector, prob:Vector) =>
          println(s"($label, $features) --> prob=$prob, predicted Label=$predictedLabel")
      }

    val evaluator = new MulticlassClassificationEvaluator().
      setLabelCol("indexedLabel").
      setPredictionCol("prediction")

    val lrAccuracy = evaluator.evaluate(lrPredictions)
    println("evaluete 结果",lrAccuracy)

    val bestModel= cvModel.bestModel.asInstanceOf[PipelineModel]
    val lrModel = bestModel.stages(2).
      asInstanceOf[LogisticRegressionModel]
    println("Coefficients: " + lrModel.coefficientMatrix + "Intercept: "+lrModel.interceptVector+ "numClasses: "+lrModel.numClasses+"numFeatures: "+lrModel.numFeatures)
    println(lrModel.explainParam(lrModel.regParam))
    println(lrModel.explainParam(lrModel.elasticNetParam))
  }
}
