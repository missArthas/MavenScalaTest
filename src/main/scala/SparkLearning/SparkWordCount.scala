package SparkLearning

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shhuang on 2017/2/28.
  */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val logFile = "data/test.txt"  // Should be some file on your server.
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(logFile).cache()

    var wordCount1=lines.flatMap(_.split(" "))
    var wordCount2=wordCount1.map((_, 1))
    var wordCount=wordCount2.reduceByKey(_+_).collect()
    println("word count result:")
    for (i <- 0 to wordCount.length - 1) {
      println(wordCount(i)._1 + "\t" + wordCount(i)._2)
    }

  }

}
