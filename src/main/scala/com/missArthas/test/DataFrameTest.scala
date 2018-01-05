package com.missArthas.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}


object DataFrameTest {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val logFile = "data/behavior.txt" // Should be some file on your server.
    val ratingsFile = "data/MovieLens/ratings.dat"
    val usersFile = "data/MovieLens/users.dat"
    val moviesFile = "data/MovieLens/movies.dat"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tmpRatings = sqlContext.read.text(ratingsFile).as[String]
    val tmpMovies = sqlContext.read.text(moviesFile).as[String]

    val ratingDF = tmpRatings.map{line =>
      val fields = line.split("::")
      (fields(0).toInt,fields(1).toInt,fields(2).toInt,fields(3))
      }
      .toDF("uid","mid","score","time")

    val moviesDF = tmpMovies.map{ line =>
      val fields =line.split("::")
      (fields(0).toInt,fields(1))
    }.toDF("mid","mname")


    val table1 = sqlContext.createDataFrame(
      List(
        (1, "A1"),
        (2, "A2"),
        (3, "A3"),
        (4, "A4"),
        (5, "A5"),
        (6, "A6"),
        (7, "A7"),
        (8, "A8"),
        (2, "A9"))
    ).toDF("aid","aname")

    val table2 = sqlContext.createDataFrame(
      List(
        (100, 2),
        (101, 6),
        (102, 3),
        (103, 7),
        (104, 6),
        (105, 8),
        (106, 2),
        (107, 2),
        (108, 11))
    ).toDF("bnum","bid")

    println("default")
    println(table1.join(table2, table1("aid") === table2("bid")).show())

    println("inner")
    println(table1.join(table2, table1("aid") === table2("bid"), "inner").show())

    println("left_outer")
    println(table1.join(table2, table1("aid") === table2("bid"), "left_outer").show())

    println("right_outer")
    println(table1.join(table2, table1("aid") === table2("bid"), "right_outer").show())

    println("udf1")
    val t1 = udf((num: Int, bid: Int) => {
      bid > 5
    })
    println(table2.where(t1($"bnum",$"bid")).show())

    println("udf2")
    sqlContext.udf.register("t2", (num: Int, bid: Int) => {
      bid > 4
    })
    println(table2.selectExpr("t2(bnum, bid)").show())
    /*
    println(ratingDF.show())
    println(moviesDF.show())

    println(ratingDF.describe("uid", "mid", "score").show())

    println(ratingDF.selectExpr("uid as userID", "mid as itemID").show())

    println(ratingDF.where("uid = 1").show())

    println(ratingDF.join(moviesDF, "mid").show())
    */
  }
}
