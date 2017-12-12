
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by shhuang on 2017/2/24.
  */


object SparkHelloWorld {

  def main(args: Array[String]) {
    val logFile = "data/test.txt"  // Should be some file on your server.
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()

    val numAs = logData.filter(line => line.contains("h")).count()
    val numBs = logData.filter(line => line.contains("j")).count()

    //line count
    val lines = sc.textFile(logFile).cache()
    val lineLengths = lines.map(line => line).count()

    //word count
    val wordLength = lines.flatMap(_.split(" ")).count()

    //char count
    val charLengthList = lines.map(line => line.length())
    val charLength = charLengthList.reduce((a, b) => a + b)

    println ("the .txt has "+lineLengths+" lines")
    println ("the .txt has "+wordLength+" words")
    println ("the .txt has "+charLength+" chars")

    println("Lines with h: %s, Lines with j: %s".format(numAs, numBs))

  }

}
