package com.missArthas.SparkLearning

import java.io.FileWriter
import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by shhuang on 2017/3/1.
  */
object SparkPeopleHeight {

  def main(args: Array[String]): Unit = {
    createData()
    calculate()
  }

  def calculate(): Unit ={
    val logFile = "data/sample_people__height_info.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(logFile).cache()
    var counts = lines.map(line => line).count()

    val maleHeight = lines.filter(line => line.contains("M")).map(line =>line.split(" ")(2).toInt)
    val femaleHeight = lines.filter(line => line.contains("F")).map(line =>line.split(" ")(2).toInt)

    //sort height
    val maleMaxHeight=maleHeight.sortBy(x=>x,false).first()
    val maleMinHeight=maleHeight.sortBy(x=>x,true).first()

    val femaleMaxHeight=femaleHeight.sortBy(x=>x,false).first()
    val femaleMinHeight=femaleHeight.sortBy(x=>x,true).first()

    println(maleMinHeight)
    println(maleMaxHeight)

    println(femaleMinHeight)
    println(femaleMaxHeight)


    //sort line by height
    val maleMaxLine=lines.filter(line => line.contains("M")).sortBy(line=>line.split(" ")(2).toInt,false).first()
    val maleMinLine=lines.filter(line => line.contains("M")).sortBy(line=>line.split(" ")(2).toInt,true).first()

    val femaleMaxLine=lines.filter(line => line.contains("F")).sortBy(line=>line.split(" ")(2).toInt,false).first()
    val femaleMinLine=lines.filter(line => line.contains("F")).sortBy(line=>line.split(" ")(2).toInt,true).first()

    println(maleMaxLine)
    println(maleMinLine)

    println(femaleMaxLine)
    println(femaleMinLine)

  }

  def createData(): Unit ={
      val writer = new FileWriter(new File("data/sample_people__height_info.txt"),false)
      val rand = new Random()
      for ( i <- 1 to 1000) {
        var height = rand.nextInt(220)
        if (height < 50) {
          height = height + 50
        }
        var gender = getRandomGender
        if (height < 100 && gender == "M")
          height = height + 100
        if (height < 100 && gender == "F")
          height = height + 50
        writer.write(i+" "+getRandomGender + " " + height)
        writer.write(System.getProperty("line.separator"))
      }
      writer.flush()
      writer.close()
      println("People Information File generated successfully.")
    }

    def getRandomGender() :String = {
      val rand = new Random()
      val randNum = rand.nextInt(2) + 1
      if (randNum % 2 == 0) {
        "M"
      } else {
        "F"
      }
    }
}
