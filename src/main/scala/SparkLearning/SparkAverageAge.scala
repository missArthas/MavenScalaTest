package SparkLearning

import java.io.{File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by shhuang on 2017/3/1.
  */
object SparkAverageAge {
  def main(args: Array[String]): Unit = {
    initData()
    calAverageAge()
  }

  def calAverageAge(): Unit = {
    val logFile = "data/sample_age_data.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(logFile).cache()
    var counts = lines.map(line => line).count()

    val ageList = lines.map(line => line.split(" ")(1)).map(age => age.toDouble)
    val ageSum = ageList.reduce((a, b) => a + b)
    println("average of 1000 people: " + ageSum / counts)
  }

  def initData(): Unit = {
    val writer = new FileWriter(new File("data/sample_age_data.txt"), false)
    val rand = new Random()
    for (i <- 1 to 1000) {
      writer.write(i + " " + rand.nextInt(100))
      writer.write(System.getProperty("line.separator"))
    }
    writer.flush()
    writer.close()
  }
}
