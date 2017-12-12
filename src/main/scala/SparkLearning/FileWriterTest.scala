package SparkLearning

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object FileWriterTest {
  def main(args: Array[String]): Unit = {
    val starttime=System.nanoTime
    val logFile = "data/behavior.txt" // Should be some file on your server.
    val movielens = "data/MovieLens/ratings.dat"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val srcData = sc.textFile(logFile, 2).cache()
    val ratingsFile = sc.textFile(movielens, 2).cache()

    val usrAlbumScore = srcData.map { line =>
      val fields = line.split(" ")
      (fields(0), fields(1).toInt)
    }

    val ratings = ratingsFile.map { line =>
      var fields = line.split("::")
      (fields(0), fields(1).toInt, fields(2).toDouble)
    }

    val writer = new PrintWriter(new File("learningScala.txt"))
    writer.println("ok")
    for((uid:String, iid:Int, rat:Double) <- ratings){
      writer.println(uid,iid,rat)
    }
    val endtime=System.nanoTime
    writer.println("run time:", (endtime - starttime)/1000000000)
    writer.close()
  }
}
