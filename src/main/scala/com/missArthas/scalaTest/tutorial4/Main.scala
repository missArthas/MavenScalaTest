package com.missArthas.scalaTest.tutorial4

import java.io.File
import java.util.Scanner
import java.util.TreeMap

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.io.Source

/**
  * Created by shhuang on 2017/3/15.
  */
object Main {
  def main(args: Array[String]): Unit = {
    //exe 1
    var equips = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0)
    val discountEquips = for ((k: String, v: Double) <- equips) yield (k, v * 0.9)
    val max=(a:Int,b:Int)=>{
      if(a>b) a
      else b
    }
    println(max(1,3))
    println(equips)
    println(discountEquips)

    exe2()
    exe3()
    exe4()
    //exe5()

    //exe10
    println("Hello".zip("wordl"))
  }

    //exe 2
    def exe2():Unit={
      val in=new Scanner(new File("data/test.txt"))
      var wordsCount=new mutable.HashMap[String,Int]()
      while(in.hasNext()) {
        val temp = in.next()
        wordsCount(temp) = wordsCount.getOrElse(temp, 0) + 1
      }
      println(wordsCount)
    }

    //exe 3
  def exe3():Unit={
    val in=new Scanner(new File("data/test.txt"))
    val wordsCount=mutable.HashMap[String,Int]()
    val result=mutable.HashMap[String,Int]()
    while(in.hasNext()) {
      val temp = in.next()
      result+=(temp -> (result.getOrElse(temp,0)+1))
    }
    println(result.toString())
  }

  //exe 4
  def exe4():Unit={
    val in=new Scanner(new File("data/test.txt"))
    var result=SortedMap[String,Int]()
    while(in.hasNext()) {
      val temp = in.next()
      result+=(temp -> (result.getOrElse(temp,0)+1))
    }
    println(result)
  }

  //exe 5
//  def exe5():Unit={
//    val source = Source.fromFile("myfile.txt").mkString
//    val tokens = source.split("\\s+")
//    val map:Map[String,Int] = new TreeMap[String,Int]
//    for(key <- tokens) {
//      map(key) = map.getOrElse(key,0) + 1
//    }
//    println(map.mkString(","))
//  }

  //exe9
//  def exe9(values:Array[Int],v:Int):Array[Int] ={
//  }

}
