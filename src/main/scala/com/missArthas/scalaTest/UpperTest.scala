package com.missArthas.scalaTest

/**
  * Created by shhuang on 2017/3/13.
  */
object UpperTest {
  def main(args: Array[String]): Unit = {
    val strings=("hello","world")
    println(UpperTest.upper("hello","world"))
    println(UpperTest.upper("hello","world").mkString(" "))
    println(UpperTest.upper("hello","world").mkString("[",",","]"))
    println("hello","world")
  }
  def upper(s:String*)=s.map(_.toUpperCase())
}


