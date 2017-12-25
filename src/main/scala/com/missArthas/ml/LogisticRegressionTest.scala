package com.missArthas.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.dmg.pmml.False

object LogisticRegressionTest {
  Logger.getLogger("org").setLevel(Level.ERROR)
  /**
    * 生成数据
    * */
  def generateData(spark: SparkSession):DataFrame = {
    import spark.implicits._
    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5),
      (0, "male", 37, 15, "yes", 2, 20, 7, 2),
      (0, "male", 27, 4, "yes", 4, 18, 6, 4),
      (0, "male", 47, 15, "yes", 5, 17, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 17, 5, 4),
      (0, "female", 27, 4, "no", 4, 14, 5, 4),
      (0, "female", 37, 15, "yes", 1, 17, 5, 5),
      (0, "female", 37, 15, "yes", 2, 18, 4, 3),
      (0, "female", 22, 0.75, "no", 3, 16, 5, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 10, "yes", 2, 14, 1, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 10, "yes", 4, 16, 5, 4),
      (0, "female", 32, 10, "yes", 3, 14, 1, 5),
      (0, "male", 37, 4, "yes", 2, 20, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 18, 5, 5),
      (0, "female", 27, 7, "no", 4, 16, 1, 5),
      (0, "male", 42, 15, "yes", 5, 20, 6, 4),
      (0, "male", 27, 4, "yes", 3, 16, 5, 5),
      (0, "female", 27, 4, "yes", 3, 17, 5, 4),
      (0, "male", 42, 15, "yes", 4, 20, 6, 3),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 27, 0.417, "no", 4, 17, 6, 4),
      (0, "female", 42, 15, "yes", 5, 14, 5, 4),
      (0, "male", 32, 4, "yes", 1, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 4, 16, 5, 3),
      (0, "female", 42, 15, "yes", 3, 12, 1, 4),
      (0, "female", 22, 4, "no", 4, 17, 5, 5),
      (0, "male", 22, 1.5, "yes", 1, 14, 3, 5),
      (0, "female", 22, 0.75, "no", 3, 16, 1, 5),
      (0, "male", 32, 10, "yes", 5, 20, 6, 5),
      (0, "male", 52, 15, "yes", 5, 18, 6, 3),
      (0, "female", 22, 0.417, "no", 5, 14, 1, 4),
      (0, "female", 27, 4, "yes", 2, 18, 6, 1),
      (0, "female", 32, 7, "yes", 5, 17, 5, 3),
      (0, "male", 22, 4, "no", 3, 16, 5, 5),
      (0, "female", 27, 7, "yes", 4, 18, 6, 5),
      (0, "female", 42, 15, "yes", 2, 18, 5, 4),
      (0, "male", 27, 1.5, "yes", 4, 16, 3, 5),
      (0, "male", 42, 15, "yes", 2, 20, 6, 4),
      (0, "female", 22, 0.75, "no", 5, 14, 3, 5),
      (0, "male", 32, 7, "yes", 2, 20, 6, 4),
      (0, "male", 27, 4, "yes", 5, 20, 6, 5),
      (0, "male", 27, 10, "yes", 4, 20, 6, 4),
      (0, "male", 22, 4, "no", 1, 18, 5, 5),
      (0, "female", 37, 15, "yes", 4, 14, 3, 1),
      (0, "male", 22, 1.5, "yes", 5, 16, 4, 4),
      (0, "female", 37, 15, "yes", 4, 17, 1, 5),
      (0, "female", 27, 0.75, "no", 4, 17, 5, 4),
      (0, "male", 32, 10, "yes", 4, 20, 6, 4),
      (0, "female", 47, 15, "yes", 5, 14, 7, 2),
      (0, "male", 37, 10, "yes", 3, 20, 6, 4),
      (0, "female", 22, 0.75, "no", 2, 16, 5, 5),
      (0, "male", 27, 4, "no", 2, 18, 4, 5),
      (0, "male", 32, 7, "no", 4, 20, 6, 4),
      (0, "male", 42, 15, "yes", 2, 17, 3, 5),
      (0, "male", 37, 10, "yes", 4, 20, 6, 4),
      (0, "female", 47, 15, "yes", 3, 17, 6, 5),
      (0, "female", 22, 1.5, "no", 5, 16, 5, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 6, 4),
      (0, "female", 27, 4, "no", 3, 17, 5, 5),
      (0, "female", 32, 10, "yes", 5, 14, 4, 5),
      (0, "female", 22, 0.125, "no", 2, 12, 5, 5),
      (0, "male", 47, 15, "yes", 4, 14, 4, 3),
      (0, "male", 32, 15, "yes", 1, 14, 5, 5),
      (0, "male", 27, 7, "yes", 4, 16, 5, 5),
      (0, "female", 22, 1.5, "yes", 3, 16, 5, 5),
      (0, "male", 27, 4, "yes", 3, 17, 6, 5),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 57, 15, "yes", 2, 14, 7, 2),
      (0, "male", 17.5, 1.5, "yes", 3, 18, 6, 5),
      (0, "male", 57, 15, "yes", 4, 20, 6, 5),
      (0, "female", 22, 0.75, "no", 2, 16, 3, 4),
      (0, "male", 42, 4, "no", 4, 17, 3, 3),
      (0, "female", 22, 1.5, "yes", 4, 12, 1, 5),
      (0, "female", 22, 0.417, "no", 1, 17, 6, 4),
      (0, "female", 32, 15, "yes", 4, 17, 5, 5),
      (0, "female", 27, 1.5, "no", 3, 18, 5, 2),
      (0, "female", 22, 1.5, "yes", 3, 14, 1, 5),
      (0, "female", 37, 15, "yes", 3, 14, 1, 4),
      (0, "female", 32, 15, "yes", 4, 14, 3, 4),
      (0, "male", 37, 10, "yes", 2, 14, 5, 3),
      (0, "male", 37, 10, "yes", 4, 16, 5, 4),
      (0, "male", 57, 15, "yes", 5, 20, 5, 3),
      (0, "male", 27, 0.417, "no", 1, 16, 3, 4),
      (0, "female", 42, 15, "yes", 5, 14, 1, 5),
      (0, "male", 57, 15, "yes", 3, 16, 6, 1),
      (0, "male", 37, 10, "yes", 1, 16, 6, 4),
      (0, "male", 37, 15, "yes", 3, 17, 5, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 5),
      (0, "female", 27, 10, "yes", 5, 14, 1, 5),
      (0, "male", 37, 10, "yes", 2, 18, 6, 4),
      (0, "female", 22, 0.125, "no", 4, 12, 4, 5),
      (0, "male", 57, 15, "yes", 5, 20, 6, 5),
      (0, "female", 37, 15, "yes", 4, 18, 6, 4),
      (0, "male", 22, 4, "yes", 4, 14, 6, 4),
      (0, "male", 27, 7, "yes", 4, 18, 5, 4),
      (0, "male", 57, 15, "yes", 4, 20, 5, 4),
      (0, "male", 32, 15, "yes", 3, 14, 6, 3),
      (0, "female", 22, 1.5, "no", 2, 14, 5, 4),
      (0, "female", 32, 7, "yes", 4, 17, 1, 5),
      (0, "female", 37, 15, "yes", 4, 17, 6, 5),
      (0, "female", 32, 1.5, "no", 5, 18, 5, 5),
      (0, "male", 42, 10, "yes", 5, 20, 7, 4),
      (0, "female", 27, 7, "no", 3, 16, 5, 4),
      (0, "male", 37, 15, "no", 4, 20, 6, 5),
      (0, "male", 37, 15, "yes", 4, 14, 3, 2),
      (0, "male", 32, 10, "no", 5, 18, 6, 4),
      (0, "female", 22, 0.75, "no", 4, 16, 1, 5),
      (0, "female", 27, 7, "yes", 4, 12, 2, 4),
      (0, "female", 27, 7, "yes", 2, 16, 2, 5),
      (0, "female", 42, 15, "yes", 5, 18, 5, 4),
      (0, "male", 42, 15, "yes", 4, 17, 5, 3),
      (0, "female", 27, 7, "yes", 2, 16, 1, 2),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 5),
      (0, "male", 37, 15, "yes", 5, 20, 6, 5),
      (0, "female", 22, 0.125, "no", 2, 14, 4, 5),
      (0, "male", 27, 1.5, "no", 4, 16, 5, 5),
      (0, "male", 32, 1.5, "no", 2, 18, 6, 5),
      (0, "male", 27, 1.5, "no", 2, 17, 6, 5),
      (0, "female", 27, 10, "yes", 4, 16, 1, 3),
      (0, "male", 42, 15, "yes", 4, 18, 6, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 6, 5),
      (0, "male", 27, 4, "no", 2, 18, 6, 3),
      (0, "female", 32, 10, "yes", 3, 14, 5, 3),
      (0, "female", 32, 15, "yes", 3, 18, 5, 4),
      (0, "female", 22, 0.75, "no", 2, 18, 6, 5),
      (0, "female", 37, 15, "yes", 2, 16, 1, 4),
      (0, "male", 27, 4, "yes", 4, 20, 5, 5),
      (0, "male", 27, 4, "no", 1, 20, 5, 4),
      (0, "female", 27, 10, "yes", 2, 12, 1, 4),
      (0, "female", 32, 15, "yes", 5, 18, 6, 4),
      (0, "male", 27, 7, "yes", 5, 12, 5, 3),
      (0, "male", 52, 15, "yes", 2, 18, 5, 4),
      (0, "male", 27, 4, "no", 3, 20, 6, 3),
      (0, "male", 37, 4, "yes", 1, 18, 5, 4),
      (0, "male", 27, 4, "yes", 4, 14, 5, 4),
      (0, "female", 52, 15, "yes", 5, 12, 1, 3),
      (0, "female", 57, 15, "yes", 4, 16, 6, 4),
      (0, "male", 27, 7, "yes", 1, 16, 5, 4),
      (0, "male", 37, 7, "yes", 4, 20, 6, 3),
      (0, "male", 22, 0.75, "no", 2, 14, 4, 3),
      (0, "male", 32, 4, "yes", 2, 18, 5, 3),
      (0, "male", 37, 15, "yes", 4, 20, 6, 3),
      (0, "male", 22, 0.75, "yes", 2, 14, 4, 3),
      (0, "male", 42, 15, "yes", 4, 20, 6, 3),
      (0, "female", 52, 15, "yes", 5, 17, 1, 1),
      (0, "female", 37, 15, "yes", 4, 14, 1, 2),
      (0, "male", 27, 7, "yes", 4, 14, 5, 3),
      (0, "male", 32, 4, "yes", 2, 16, 5, 5),
      (0, "female", 27, 4, "yes", 2, 18, 6, 5),
      (0, "female", 27, 4, "yes", 2, 18, 5, 5),
      (0, "male", 37, 15, "yes", 5, 18, 6, 5),
      (0, "female", 47, 15, "yes", 5, 12, 5, 4),
      (0, "female", 32, 10, "yes", 3, 17, 1, 4),
      (0, "female", 27, 1.5, "yes", 4, 17, 1, 2),
      (0, "female", 57, 15, "yes", 2, 18, 5, 2),
      (0, "female", 22, 1.5, "no", 4, 14, 5, 4),
      (0, "male", 42, 15, "yes", 3, 14, 3, 4),
      (0, "male", 57, 15, "yes", 4, 9, 2, 2),
      (0, "male", 57, 15, "yes", 4, 20, 6, 5),
      (0, "female", 22, 0.125, "no", 4, 14, 4, 5),
      (0, "female", 32, 10, "yes", 4, 14, 1, 5),
      (0, "female", 42, 15, "yes", 3, 18, 5, 4),
      (0, "female", 27, 1.5, "no", 2, 18, 6, 5),
      (0, "male", 32, 0.125, "yes", 2, 18, 5, 2),
      (0, "female", 27, 4, "no", 3, 16, 5, 4),
      (0, "female", 27, 10, "yes", 2, 16, 1, 4),
      (0, "female", 32, 7, "yes", 4, 16, 1, 3),
      (0, "female", 37, 15, "yes", 4, 14, 5, 4),
      (0, "female", 42, 15, "yes", 5, 17, 6, 2),
      (0, "male", 32, 1.5, "yes", 4, 14, 6, 5),
      (0, "female", 32, 4, "yes", 3, 17, 5, 3),
      (0, "female", 37, 7, "no", 4, 18, 5, 5),
      (0, "female", 22, 0.417, "yes", 3, 14, 3, 5),
      (0, "female", 27, 7, "yes", 4, 14, 1, 5),
      (0, "male", 27, 0.75, "no", 3, 16, 5, 5),
      (0, "male", 27, 4, "yes", 2, 20, 5, 5),
      (0, "male", 32, 10, "yes", 4, 16, 4, 5),
      (0, "male", 32, 15, "yes", 1, 14, 5, 5),
      (0, "male", 22, 0.75, "no", 3, 17, 4, 5),
      (0, "female", 27, 7, "yes", 4, 17, 1, 4),
      (0, "male", 27, 0.417, "yes", 4, 20, 5, 4),
      (0, "male", 37, 15, "yes", 4, 20, 5, 4),
      (0, "female", 37, 15, "yes", 2, 14, 1, 3),
      (0, "male", 22, 4, "yes", 1, 18, 5, 4),
      (0, "male", 37, 15, "yes", 4, 17, 5, 3),
      (0, "female", 22, 1.5, "no", 2, 14, 4, 5),
      (0, "male", 52, 15, "yes", 4, 14, 6, 2),
      (0, "female", 22, 1.5, "no", 4, 17, 5, 5),
      (0, "male", 32, 4, "yes", 5, 14, 3, 5),
      (0, "male", 32, 4, "yes", 2, 14, 3, 5),
      (0, "female", 22, 1.5, "no", 3, 16, 6, 5),
      (0, "male", 27, 0.75, "no", 2, 18, 3, 3),
      (0, "female", 22, 7, "yes", 2, 14, 5, 2),
      (0, "female", 27, 0.75, "no", 2, 17, 5, 3),
      (0, "female", 37, 15, "yes", 4, 12, 1, 2),
      (0, "female", 22, 1.5, "no", 1, 14, 1, 5),
      (0, "female", 37, 10, "no", 2, 12, 4, 4),
      (0, "female", 37, 15, "yes", 4, 18, 5, 3),
      (0, "female", 42, 15, "yes", 3, 12, 3, 3),
      (0, "male", 22, 4, "no", 2, 18, 5, 5),
      (0, "male", 52, 7, "yes", 2, 20, 6, 2),
      (0, "male", 27, 0.75, "no", 2, 17, 5, 5),
      (0, "female", 27, 4, "no", 2, 17, 4, 5),
      (0, "male", 42, 1.5, "no", 5, 20, 6, 5),
      (0, "male", 22, 1.5, "no", 4, 17, 6, 5),
      (0, "male", 22, 4, "no", 4, 17, 5, 3),
      (0, "female", 22, 4, "yes", 1, 14, 5, 4),
      (0, "male", 37, 15, "yes", 5, 20, 4, 5),
      (0, "female", 37, 10, "yes", 3, 16, 6, 3),
      (0, "male", 42, 15, "yes", 4, 17, 6, 5),
      (0, "female", 47, 15, "yes", 4, 17, 5, 5),
      (0, "male", 22, 1.5, "no", 4, 16, 5, 4),
      (0, "female", 32, 10, "yes", 3, 12, 1, 4),
      (0, "female", 22, 7, "yes", 1, 14, 3, 5),
      (0, "female", 32, 10, "yes", 4, 17, 5, 4),
      (0, "male", 27, 1.5, "yes", 2, 16, 2, 4),
      (0, "male", 37, 15, "yes", 4, 14, 5, 5),
      (0, "male", 42, 4, "yes", 3, 14, 4, 5),
      (0, "female", 37, 15, "yes", 5, 14, 5, 4),
      (0, "female", 32, 7, "yes", 4, 17, 5, 5),
      (0, "female", 42, 15, "yes", 4, 18, 6, 5),
      (0, "male", 27, 4, "no", 4, 18, 6, 4),
      (0, "male", 22, 0.75, "no", 4, 18, 6, 5),
      (0, "male", 27, 4, "yes", 4, 14, 5, 3),
      (0, "female", 22, 0.75, "no", 5, 18, 1, 5),
      (0, "female", 52, 15, "yes", 5, 9, 5, 5),
      (0, "male", 32, 10, "yes", 3, 14, 5, 5),
      (0, "female", 37, 15, "yes", 4, 16, 4, 4),
      (0, "male", 32, 7, "yes", 2, 20, 5, 4),
      (0, "female", 42, 15, "yes", 3, 18, 1, 4),
      (0, "male", 32, 15, "yes", 1, 16, 5, 5),
      (0, "male", 27, 4, "yes", 3, 18, 5, 5),
      (0, "female", 32, 15, "yes", 4, 12, 3, 4),
      (0, "male", 22, 0.75, "yes", 3, 14, 2, 4),
      (0, "female", 22, 1.5, "no", 3, 16, 5, 3),
      (0, "female", 42, 15, "yes", 4, 14, 3, 5),
      (0, "female", 52, 15, "yes", 3, 16, 5, 4),
      (0, "male", 37, 15, "yes", 5, 20, 6, 4),
      (0, "female", 47, 15, "yes", 4, 12, 2, 3),
      (0, "male", 57, 15, "yes", 2, 20, 6, 4),
      (0, "male", 32, 7, "yes", 4, 17, 5, 5),
      (0, "female", 27, 7, "yes", 4, 17, 1, 4),
      (0, "male", 22, 1.5, "no", 1, 18, 6, 5),
      (0, "female", 22, 4, "yes", 3, 9, 1, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 1, 5),
      (0, "male", 42, 15, "yes", 2, 20, 6, 4),
      (0, "male", 57, 15, "yes", 4, 9, 2, 4),
      (0, "female", 27, 7, "yes", 2, 18, 1, 5),
      (0, "female", 22, 4, "yes", 3, 14, 1, 5),
      (0, "male", 37, 15, "yes", 4, 14, 5, 3),
      (0, "male", 32, 7, "yes", 1, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 5, 5),
      (0, "female", 22, 1.5, "yes", 3, 12, 1, 3),
      (0, "male", 52, 15, "yes", 2, 14, 5, 5),
      (0, "female", 37, 15, "yes", 2, 14, 1, 1),
      (0, "female", 32, 10, "yes", 2, 14, 5, 5),
      (0, "male", 42, 15, "yes", 4, 20, 4, 5),
      (0, "female", 27, 4, "yes", 3, 18, 4, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 5),
      (0, "male", 27, 1.5, "no", 3, 18, 5, 5),
      (0, "female", 22, 0.125, "no", 2, 16, 6, 3),
      (0, "male", 32, 10, "yes", 2, 20, 6, 3),
      (0, "female", 27, 4, "no", 4, 18, 5, 4),
      (0, "female", 27, 7, "yes", 2, 12, 5, 1),
      (0, "male", 32, 4, "yes", 5, 18, 6, 3),
      (0, "female", 37, 15, "yes", 2, 17, 5, 5),
      (0, "male", 47, 15, "no", 4, 20, 6, 4),
      (0, "male", 27, 1.5, "no", 1, 18, 5, 5),
      (0, "male", 37, 15, "yes", 4, 20, 6, 4),
      (0, "female", 32, 15, "yes", 4, 18, 1, 4),
      (0, "female", 32, 7, "yes", 4, 17, 5, 4),
      (0, "female", 42, 15, "yes", 3, 14, 1, 3),
      (0, "female", 27, 7, "yes", 3, 16, 1, 4),
      (0, "male", 27, 1.5, "no", 3, 16, 4, 2),
      (0, "male", 22, 1.5, "no", 3, 16, 3, 5),
      (0, "male", 27, 4, "yes", 3, 16, 4, 2),
      (0, "female", 27, 7, "yes", 3, 12, 1, 2),
      (0, "female", 37, 15, "yes", 2, 18, 5, 4),
      (0, "female", 37, 7, "yes", 3, 14, 4, 4),
      (0, "male", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "male", 37, 15, "yes", 5, 20, 5, 4),
      (0, "female", 22, 1.5, "no", 4, 16, 5, 3),
      (0, "female", 32, 10, "yes", 4, 16, 1, 5),
      (0, "male", 27, 4, "no", 2, 17, 5, 3),
      (0, "female", 22, 0.417, "no", 4, 14, 5, 5),
      (0, "female", 27, 4, "no", 2, 18, 5, 5),
      (0, "male", 37, 15, "yes", 4, 18, 5, 3),
      (0, "male", 37, 10, "yes", 5, 20, 7, 4),
      (0, "female", 27, 7, "yes", 2, 14, 4, 2),
      (0, "male", 32, 4, "yes", 2, 16, 5, 5),
      (0, "male", 32, 4, "yes", 2, 16, 6, 4),
      (0, "male", 22, 1.5, "no", 3, 18, 4, 5),
      (0, "female", 22, 4, "yes", 4, 14, 3, 4),
      (0, "female", 17.5, 0.75, "no", 2, 18, 5, 4),
      (0, "male", 32, 10, "yes", 4, 20, 4, 5),
      (0, "female", 32, 0.75, "no", 5, 14, 3, 3),
      (0, "male", 37, 15, "yes", 4, 17, 5, 3),
      (0, "male", 32, 4, "no", 3, 14, 4, 5),
      (0, "female", 27, 1.5, "no", 2, 17, 3, 2),
      (0, "female", 22, 7, "yes", 4, 14, 1, 5),
      (0, "male", 47, 15, "yes", 5, 14, 6, 5),
      (0, "male", 27, 4, "yes", 1, 16, 4, 4),
      (0, "female", 37, 15, "yes", 5, 14, 1, 3),
      (0, "male", 42, 4, "yes", 4, 18, 5, 5),
      (0, "female", 32, 4, "yes", 2, 14, 1, 5),
      (0, "male", 52, 15, "yes", 2, 14, 7, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 1, 4),
      (0, "male", 52, 15, "yes", 4, 12, 2, 4),
      (0, "female", 22, 0.417, "no", 3, 17, 1, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "male", 27, 4, "yes", 4, 20, 6, 4),
      (0, "female", 32, 15, "yes", 4, 14, 1, 5),
      (0, "female", 27, 1.5, "no", 2, 16, 3, 5),
      (0, "male", 32, 4, "no", 1, 20, 6, 5),
      (0, "male", 37, 15, "yes", 3, 20, 6, 4),
      (0, "female", 32, 10, "no", 2, 16, 6, 5),
      (0, "female", 32, 10, "yes", 5, 14, 5, 5),
      (0, "male", 37, 1.5, "yes", 4, 18, 5, 3),
      (0, "male", 32, 1.5, "no", 2, 18, 4, 4),
      (0, "female", 32, 10, "yes", 4, 14, 1, 4),
      (0, "female", 47, 15, "yes", 4, 18, 5, 4),
      (0, "female", 27, 10, "yes", 5, 12, 1, 5),
      (0, "male", 27, 4, "yes", 3, 16, 4, 5),
      (0, "female", 37, 15, "yes", 4, 12, 4, 2),
      (0, "female", 27, 0.75, "no", 4, 16, 5, 5),
      (0, "female", 37, 15, "yes", 4, 16, 1, 5),
      (0, "female", 32, 15, "yes", 3, 16, 1, 5),
      (0, "female", 27, 10, "yes", 2, 16, 1, 5),
      (0, "male", 27, 7, "no", 2, 20, 6, 5),
      (0, "female", 37, 15, "yes", 2, 14, 1, 3),
      (0, "male", 27, 1.5, "yes", 2, 17, 4, 4),
      (0, "female", 22, 0.75, "yes", 2, 14, 1, 5),
      (0, "male", 22, 4, "yes", 4, 14, 2, 4),
      (0, "male", 42, 0.125, "no", 4, 17, 6, 4),
      (0, "male", 27, 1.5, "yes", 4, 18, 6, 5),
      (0, "male", 27, 7, "yes", 3, 16, 6, 3),
      (0, "female", 52, 15, "yes", 4, 14, 1, 3),
      (0, "male", 27, 1.5, "no", 5, 20, 5, 2),
      (0, "female", 27, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 27, 1.5, "no", 3, 17, 5, 5),
      (0, "male", 22, 0.125, "no", 5, 16, 4, 4),
      (0, "female", 27, 4, "yes", 4, 16, 1, 5),
      (0, "female", 27, 4, "yes", 4, 12, 1, 5),
      (0, "female", 47, 15, "yes", 2, 14, 5, 5),
      (0, "female", 32, 15, "yes", 3, 14, 5, 3),
      (0, "male", 42, 7, "yes", 2, 16, 5, 5),
      (0, "male", 22, 0.75, "no", 4, 16, 6, 4),
      (0, "male", 27, 0.125, "no", 3, 20, 6, 5),
      (0, "male", 32, 10, "yes", 3, 20, 6, 5),
      (0, "female", 22, 0.417, "no", 5, 14, 4, 5),
      (0, "female", 47, 15, "yes", 5, 14, 1, 4),
      (0, "female", 32, 10, "yes", 3, 14, 1, 5),
      (0, "male", 57, 15, "yes", 4, 17, 5, 5),
      (0, "male", 27, 4, "yes", 3, 20, 6, 5),
      (0, "female", 32, 7, "yes", 4, 17, 1, 5),
      (0, "female", 37, 10, "yes", 4, 16, 1, 5),
      (0, "female", 32, 10, "yes", 1, 18, 1, 4),
      (0, "female", 22, 4, "no", 3, 14, 1, 4),
      (0, "female", 27, 7, "yes", 4, 14, 3, 2),
      (0, "male", 57, 15, "yes", 5, 18, 5, 2),
      (0, "male", 32, 7, "yes", 2, 18, 5, 5),
      (0, "female", 27, 1.5, "no", 4, 17, 1, 3),
      (0, "male", 22, 1.5, "no", 4, 14, 5, 5),
      (0, "female", 22, 1.5, "yes", 4, 14, 5, 4),
      (0, "female", 32, 7, "yes", 3, 16, 1, 5),
      (0, "female", 47, 15, "yes", 3, 16, 5, 4),
      (0, "female", 22, 0.75, "no", 3, 16, 1, 5),
      (0, "female", 22, 1.5, "yes", 2, 14, 5, 5),
      (0, "female", 27, 4, "yes", 1, 16, 5, 5),
      (0, "male", 52, 15, "yes", 4, 16, 5, 5),
      (0, "male", 32, 10, "yes", 4, 20, 6, 5),
      (0, "male", 47, 15, "yes", 4, 16, 6, 4),
      (0, "female", 27, 7, "yes", 2, 14, 1, 2),
      (0, "female", 22, 1.5, "no", 4, 14, 4, 5),
      (0, "female", 32, 10, "yes", 2, 16, 5, 4),
      (0, "female", 22, 0.75, "no", 2, 16, 5, 4),
      (0, "female", 22, 1.5, "no", 2, 16, 5, 5),
      (0, "female", 42, 15, "yes", 3, 18, 6, 4),
      (0, "female", 27, 7, "yes", 5, 14, 4, 5),
      (0, "male", 42, 15, "yes", 4, 16, 4, 4),
      (0, "female", 57, 15, "yes", 3, 18, 5, 2),
      (0, "male", 42, 15, "yes", 3, 18, 6, 2),
      (0, "female", 32, 7, "yes", 2, 14, 1, 2),
      (0, "male", 22, 4, "no", 5, 12, 4, 5),
      (0, "female", 22, 1.5, "no", 1, 16, 6, 5),
      (0, "female", 22, 0.75, "no", 1, 14, 4, 5),
      (0, "female", 32, 15, "yes", 4, 12, 1, 5),
      (0, "male", 22, 1.5, "no", 2, 18, 5, 3),
      (0, "male", 27, 4, "yes", 5, 17, 2, 5),
      (0, "female", 27, 4, "yes", 4, 12, 1, 5),
      (0, "male", 42, 15, "yes", 5, 18, 5, 4),
      (0, "male", 32, 1.5, "no", 2, 20, 7, 3),
      (0, "male", 57, 15, "no", 4, 9, 3, 1),
      (0, "male", 37, 7, "no", 4, 18, 5, 5),
      (0, "male", 52, 15, "yes", 2, 17, 5, 4),
      (0, "male", 47, 15, "yes", 4, 17, 6, 5),
      (0, "female", 27, 7, "no", 2, 17, 5, 4),
      (0, "female", 27, 7, "yes", 4, 14, 5, 5),
      (0, "female", 22, 4, "no", 2, 14, 3, 3),
      (0, "male", 37, 7, "yes", 2, 20, 6, 5),
      (0, "male", 27, 7, "no", 4, 12, 4, 3),
      (0, "male", 42, 10, "yes", 4, 18, 6, 4),
      (0, "female", 22, 1.5, "no", 3, 14, 1, 5),
      (0, "female", 22, 4, "yes", 2, 14, 1, 3),
      (0, "female", 57, 15, "no", 4, 20, 6, 5),
      (0, "male", 37, 15, "yes", 4, 14, 4, 3),
      (0, "female", 27, 7, "yes", 3, 18, 5, 5),
      (0, "female", 17.5, 10, "no", 4, 14, 4, 5),
      (0, "male", 22, 4, "yes", 4, 16, 5, 5),
      (0, "female", 27, 4, "yes", 2, 16, 1, 4),
      (0, "female", 37, 15, "yes", 2, 14, 5, 1),
      (0, "female", 22, 1.5, "no", 5, 14, 1, 4),
      (0, "male", 27, 7, "yes", 2, 20, 5, 4),
      (0, "male", 27, 4, "yes", 4, 14, 5, 5),
      (0, "male", 22, 0.125, "no", 1, 16, 3, 5),
      (0, "female", 27, 7, "yes", 4, 14, 1, 4),
      (0, "female", 32, 15, "yes", 5, 16, 5, 3),
      (0, "male", 32, 10, "yes", 4, 18, 5, 4),
      (0, "female", 32, 15, "yes", 2, 14, 3, 4),
      (0, "female", 22, 1.5, "no", 3, 17, 5, 5),
      (0, "male", 27, 4, "yes", 4, 17, 4, 4),
      (0, "female", 52, 15, "yes", 5, 14, 1, 5),
      (0, "female", 27, 7, "yes", 2, 12, 1, 2),
      (0, "female", 27, 7, "yes", 3, 12, 1, 4),
      (0, "female", 42, 15, "yes", 2, 14, 1, 4),
      (0, "female", 42, 15, "yes", 4, 14, 5, 4),
      (0, "male", 27, 7, "yes", 4, 14, 3, 3),
      (0, "male", 27, 7, "yes", 2, 20, 6, 2),
      (0, "female", 42, 15, "yes", 3, 12, 3, 3),
      (0, "male", 27, 4, "yes", 3, 16, 3, 5),
      (0, "female", 27, 7, "yes", 3, 14, 1, 4),
      (0, "female", 22, 1.5, "no", 2, 14, 4, 5),
      (0, "female", 27, 4, "yes", 4, 14, 1, 4),
      (0, "female", 22, 4, "no", 4, 14, 5, 5),
      (0, "female", 22, 1.5, "no", 2, 16, 4, 5),
      (0, "male", 47, 15, "no", 4, 14, 5, 4),
      (0, "male", 37, 10, "yes", 2, 18, 6, 2),
      (0, "male", 37, 15, "yes", 3, 17, 5, 4),
      (0, "female", 27, 4, "yes", 2, 16, 1, 4),
      (3, "male", 27, 1.5, "no", 3, 18, 4, 4),
      (3, "female", 27, 4, "yes", 3, 17, 1, 5),
      (7, "male", 37, 15, "yes", 5, 18, 6, 2),
      (12, "female", 32, 10, "yes", 3, 17, 5, 2),
      (1, "male", 22, 0.125, "no", 4, 16, 5, 5),
      (1, "female", 22, 1.5, "yes", 2, 14, 1, 5),
      (12, "male", 37, 15, "yes", 4, 14, 5, 2),
      (7, "female", 22, 1.5, "no", 2, 14, 3, 4),
      (2, "male", 37, 15, "yes", 2, 18, 6, 4),
      (3, "female", 32, 15, "yes", 4, 12, 3, 2),
      (1, "female", 37, 15, "yes", 4, 14, 4, 2),
      (7, "female", 42, 15, "yes", 3, 17, 1, 4),
      (12, "female", 42, 15, "yes", 5, 9, 4, 1),
      (12, "male", 37, 10, "yes", 2, 20, 6, 2),
      (12, "female", 32, 15, "yes", 3, 14, 1, 2),
      (3, "male", 27, 4, "no", 1, 18, 6, 5),
      (7, "male", 37, 10, "yes", 2, 18, 7, 3),
      (7, "female", 27, 4, "no", 3, 17, 5, 5),
      (1, "male", 42, 15, "yes", 4, 16, 5, 5),
      (1, "female", 47, 15, "yes", 5, 14, 4, 5),
      (7, "female", 27, 4, "yes", 3, 18, 5, 4),
      (1, "female", 27, 7, "yes", 5, 14, 1, 4),
      (12, "male", 27, 1.5, "yes", 3, 17, 5, 4),
      (12, "female", 27, 7, "yes", 4, 14, 6, 2),
      (3, "female", 42, 15, "yes", 4, 16, 5, 4),
      (7, "female", 27, 10, "yes", 4, 12, 7, 3),
      (1, "male", 27, 1.5, "no", 2, 18, 5, 2),
      (1, "male", 32, 4, "no", 4, 20, 6, 4),
      (1, "female", 27, 7, "yes", 3, 14, 1, 3),
      (3, "female", 32, 10, "yes", 4, 14, 1, 4),
      (3, "male", 27, 4, "yes", 2, 18, 7, 2),
      (1, "female", 17.5, 0.75, "no", 5, 14, 4, 5),
      (1, "female", 32, 10, "yes", 4, 18, 1, 5),
      (7, "female", 32, 7, "yes", 2, 17, 6, 4),
      (7, "male", 37, 15, "yes", 2, 20, 6, 4),
      (7, "female", 37, 10, "no", 1, 20, 5, 3),
      (12, "female", 32, 10, "yes", 2, 16, 5, 5),
      (7, "male", 52, 15, "yes", 2, 20, 6, 4),
      (7, "female", 42, 15, "yes", 1, 12, 1, 3),
      (1, "male", 52, 15, "yes", 2, 20, 6, 3),
      (2, "male", 37, 15, "yes", 3, 18, 6, 5),
      (12, "female", 22, 4, "no", 3, 12, 3, 4),
      (12, "male", 27, 7, "yes", 1, 18, 6, 2),
      (1, "male", 27, 4, "yes", 3, 18, 5, 5),
      (12, "male", 47, 15, "yes", 4, 17, 6, 5),
      (12, "female", 42, 15, "yes", 4, 12, 1, 1),
      (7, "male", 27, 4, "no", 3, 14, 3, 4),
      (7, "female", 32, 7, "yes", 4, 18, 4, 5),
      (1, "male", 32, 0.417, "yes", 3, 12, 3, 4),
      (3, "male", 47, 15, "yes", 5, 16, 5, 4),
      (12, "male", 37, 15, "yes", 2, 20, 5, 4),
      (7, "male", 22, 4, "yes", 2, 17, 6, 4),
      (1, "male", 27, 4, "no", 2, 14, 4, 5),
      (7, "female", 52, 15, "yes", 5, 16, 1, 3),
      (1, "male", 27, 4, "no", 3, 14, 3, 3),
      (1, "female", 27, 10, "yes", 4, 16, 1, 4),
      (1, "male", 32, 7, "yes", 3, 14, 7, 4),
      (7, "male", 32, 7, "yes", 2, 18, 4, 1),
      (3, "male", 22, 1.5, "no", 1, 14, 3, 2),
      (7, "male", 22, 4, "yes", 3, 18, 6, 4),
      (7, "male", 42, 15, "yes", 4, 20, 6, 4),
      (2, "female", 57, 15, "yes", 1, 18, 5, 4),
      (7, "female", 32, 4, "yes", 3, 18, 5, 2),
      (1, "male", 27, 4, "yes", 1, 16, 4, 4),
      (7, "male", 32, 7, "yes", 4, 16, 1, 4),
      (2, "male", 57, 15, "yes", 1, 17, 4, 4),
      (7, "female", 42, 15, "yes", 4, 14, 5, 2),
      (7, "male", 37, 10, "yes", 1, 18, 5, 3),
      (3, "male", 42, 15, "yes", 3, 17, 6, 1),
      (1, "female", 52, 15, "yes", 3, 14, 4, 4),
      (2, "female", 27, 7, "yes", 3, 17, 5, 3),
      (12, "male", 32, 7, "yes", 2, 12, 4, 2),
      (1, "male", 22, 4, "no", 4, 14, 2, 5),
      (3, "male", 27, 7, "yes", 3, 18, 6, 4),
      (12, "female", 37, 15, "yes", 1, 18, 5, 5),
      (7, "female", 32, 15, "yes", 3, 17, 1, 3),
      (7, "female", 27, 7, "no", 2, 17, 5, 5),
      (1, "female", 32, 7, "yes", 3, 17, 5, 3),
      (1, "male", 32, 1.5, "yes", 2, 14, 2, 4),
      (12, "female", 42, 15, "yes", 4, 14, 1, 2),
      (7, "male", 32, 10, "yes", 3, 14, 5, 4),
      (7, "male", 37, 4, "yes", 1, 20, 6, 3),
      (1, "female", 27, 4, "yes", 2, 16, 5, 3),
      (12, "female", 42, 15, "yes", 3, 14, 4, 3),
      (1, "male", 27, 10, "yes", 5, 20, 6, 5),
      (12, "male", 37, 10, "yes", 2, 20, 6, 2),
      (12, "female", 27, 7, "yes", 1, 14, 3, 3),
      (3, "female", 27, 7, "yes", 4, 12, 1, 2),
      (3, "male", 32, 10, "yes", 2, 14, 4, 4),
      (12, "female", 17.5, 0.75, "yes", 2, 12, 1, 3),
      (12, "female", 32, 15, "yes", 3, 18, 5, 4),
      (2, "female", 22, 7, "no", 4, 14, 4, 3),
      (1, "male", 32, 7, "yes", 4, 20, 6, 5),
      (7, "male", 27, 4, "yes", 2, 18, 6, 2),
      (1, "female", 22, 1.5, "yes", 5, 14, 5, 3),
      (12, "female", 32, 15, "no", 3, 17, 5, 1),
      (12, "female", 42, 15, "yes", 2, 12, 1, 2),
      (7, "male", 42, 15, "yes", 3, 20, 5, 4),
      (12, "male", 32, 10, "no", 2, 18, 4, 2),
      (12, "female", 32, 15, "yes", 3, 9, 1, 1),
      (7, "male", 57, 15, "yes", 5, 20, 4, 5),
      (12, "male", 47, 15, "yes", 4, 20, 6, 4),
      (2, "female", 42, 15, "yes", 2, 17, 6, 3),
      (12, "male", 37, 15, "yes", 3, 17, 6, 3),
      (12, "male", 37, 15, "yes", 5, 17, 5, 2),
      (7, "male", 27, 10, "yes", 2, 20, 6, 4),
      (2, "male", 37, 15, "yes", 2, 16, 5, 4),
      (12, "female", 32, 15, "yes", 1, 14, 5, 2),
      (7, "male", 32, 10, "yes", 3, 17, 6, 3),
      (2, "male", 37, 15, "yes", 4, 18, 5, 1),
      (7, "female", 27, 1.5, "no", 2, 17, 5, 5),
      (3, "female", 47, 15, "yes", 2, 17, 5, 2),
      (12, "male", 37, 15, "yes", 2, 17, 5, 4),
      (12, "female", 27, 4, "no", 2, 14, 5, 5),
      (2, "female", 27, 10, "yes", 4, 14, 1, 5),
      (1, "female", 22, 4, "yes", 3, 16, 1, 3),
      (12, "male", 52, 7, "no", 4, 16, 5, 5),
      (2, "female", 27, 4, "yes", 1, 16, 3, 5),
      (7, "female", 37, 15, "yes", 2, 17, 6, 4),
      (2, "female", 27, 4, "no", 1, 17, 3, 1),
      (12, "female", 17.5, 0.75, "yes", 2, 12, 3, 5),
      (7, "female", 32, 15, "yes", 5, 18, 5, 4),
      (7, "female", 22, 4, "no", 1, 16, 3, 5),
      (2, "male", 32, 4, "yes", 4, 18, 6, 4),
      (1, "female", 22, 1.5, "yes", 3, 18, 5, 2),
      (3, "female", 42, 15, "yes", 2, 17, 5, 4),
      (1, "male", 32, 7, "yes", 4, 16, 4, 4),
      (12, "male", 37, 15, "no", 3, 14, 6, 2),
      (1, "male", 42, 15, "yes", 3, 16, 6, 3),
      (1, "male", 27, 4, "yes", 1, 18, 5, 4),
      (2, "male", 37, 15, "yes", 4, 20, 7, 3),
      (7, "male", 37, 15, "yes", 3, 20, 6, 4),
      (3, "male", 22, 1.5, "no", 2, 12, 3, 3),
      (3, "male", 32, 4, "yes", 3, 20, 6, 2),
      (2, "male", 32, 15, "yes", 5, 20, 6, 5),
      (12, "female", 52, 15, "yes", 1, 18, 5, 5),
      (12, "male", 47, 15, "no", 1, 18, 6, 5),
      (3, "female", 32, 15, "yes", 4, 16, 4, 4),
      (7, "female", 32, 15, "yes", 3, 14, 3, 2),
      (7, "female", 27, 7, "yes", 4, 16, 1, 2),
      (12, "male", 42, 15, "yes", 3, 18, 6, 2),
      (7, "female", 42, 15, "yes", 2, 14, 3, 2),
      (12, "male", 27, 7, "yes", 2, 17, 5, 4),
      (3, "male", 32, 10, "yes", 4, 14, 4, 3),
      (7, "male", 47, 15, "yes", 3, 16, 4, 2),
      (1, "male", 22, 1.5, "yes", 1, 12, 2, 5),
      (7, "female", 32, 10, "yes", 2, 18, 5, 4),
      (2, "male", 32, 10, "yes", 2, 17, 6, 5),
      (2, "male", 22, 7, "yes", 3, 18, 6, 2),
      (1, "female", 32, 15, "yes", 3, 14, 1, 5))

    val colArray1: Array[String] = Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

    dataList.toDF(colArray1: _*)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()
    import spark.implicits._
    // For implicit conversions like converting RDDs to DataFrames


    /**获取数据
      * */
    val data = generateData(spark)
    data.createOrReplaceTempView("df")

    //dataExplore(data)
    //train(spark, data)
    //auc(spark, data)
    crossValidate(spark, data)


  }

  def train(spark: SparkSession, data: DataFrame): Unit ={
    val affairs = "case when affairs>0 then 1 else 0 end as label,"
    val gender = "case when gender='female' then 0 else 1 end as gender,"
    val children = "case when children='yes' then 1 else 0 end as children,"

    val sqlDF = spark.sql("select " +
      affairs +
      gender +
      "age,yearsmarried," +
      children +
      "religiousness,education,occupation,rating" +
      " from df ")
    sqlDF.show(10)
    val colArray2 = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(sqlDF)
    val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.7, 0.3), seed = 12345)
    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(trainingDF)

    println("训练集数量：", trainingDF.count())
    println("测试集数量：", testDF.count())

    // 输出逻辑回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 设置ElasticNet混合参数,范围为[0，1]。
    // 对于α= 0，惩罚是L2惩罚。 对于alpha = 1，它是一个L1惩罚。 对于0 <α<1，惩罚是L1和L2的组合。 默认值为0.0，这是一个L2惩罚。
    println("ElasticNet混合参数:", lrModel.getElasticNetParam)

    // 正则化参数>=0
    println("正则化参数:", lrModel.getRegParam)

    // 在拟合模型之前,是否标准化特征
    println("在拟合模型之前,是否标准化特征:", lrModel.getStandardization)

    // 在二进制分类中设置阈值，范围为[0，1]。如果类标签1的估计概率>Threshold，则预测1，否则0.高阈值鼓励模型更频繁地预测0; 低阈值鼓励模型更频繁地预测1。默认值为0.5。
    println("在二进制分类中设置阈值:", lrModel.getThreshold)

    // 设置迭代的收敛容限。 较小的值将导致更高的精度与更多的迭代的成本。 默认值为1E-6。
    println("设置迭代的收敛容限getTol:", lrModel.getTol)
    println("设置迭代的收敛容限getMaxIter:", lrModel.getMaxIter)


    val testResult = lrModel.transform(testDF)

    testResult.select("features","rawPrediction","probability","prediction").show(10, false)

    val t = testResult.where("label = prediction").count()
    println("测试集正确率",t*1.0/testDF.count())
    //lrModel.transform(testDF).select("*").show(10, false)
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    val trainingSummary = lrModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))


    /**
      *
      * */

  }

  def crossValidate(spark: SparkSession, data: DataFrame): Unit ={
    val affairs = "case when affairs>0 then 1 else 0 end as label,"
    val gender = "case when gender='female' then 0 else 1 end as gender,"
    val children = "case when children='yes' then 1 else 0 end as children,"

    val sqlDF = spark.sql("select " +
      affairs +
      gender +
      "age,yearsmarried," +
      children +
      "religiousness,education,occupation,rating" +
      " from df ")
    sqlDF.show(10)
    val colArray2 = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(sqlDF)
    val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.7, 0.3), seed = 12345)
    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(trainingDF)

    println("训练集数量：", trainingDF.count())
    println("测试集数量：", testDF.count())
    val predictions = lrModel.transform(testDF)


    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val auc = evaluator.evaluate(predictions)
    println("auc:", auc)

    val t = predictions.where("label = prediction").count()
    println("测试集正确率", t*1.0/testDF.count())


    val lr = new LogisticRegression()
      .setMaxIter(10)
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept, Array(false, true))
      //.addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
      .build()

    val cv = new CrossValidator()
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)  // Use 3+ in practice


  }

  def auc(spark: SparkSession, data: DataFrame): Unit ={
    val affairs = "case when affairs>0 then 1 else 0 end as label,"
    val gender = "case when gender='female' then 0 else 1 end as gender,"
    val children = "case when children='yes' then 1 else 0 end as children,"

    val sqlDF = spark.sql("select " +
      affairs +
      gender +
      "age,yearsmarried," +
      children +
      "religiousness,education,occupation,rating" +
      " from df ")
    sqlDF.show(10)
    val colArray2 = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(sqlDF)
    val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.7, 0.3), seed = 12345)
    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(trainingDF)

    println("训练集数量：", trainingDF.count())
    println("测试集数量：", testDF.count())
    val predictions = lrModel.transform(testDF)


    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val auc = evaluator.evaluate(predictions)
    println("auc:", auc)

    val t = predictions.where("label = prediction").count()
    println("测试集正确率", t*1.0/testDF.count())


  }

  def dataExplore(data: DataFrame): Unit ={
    println("数据探索")
    println(data.describe().show())
    println(data.groupBy("rating").count().show())

  }

}
