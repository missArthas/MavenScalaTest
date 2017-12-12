package com.missArthas.scalaTest.tutorial10

/**
  * Created by shhuang on 2017/3/29.
  */
class ConsoleLogger extends Logger{
  override def log(msg: String): Unit = {
    println(msg)
  }
}
