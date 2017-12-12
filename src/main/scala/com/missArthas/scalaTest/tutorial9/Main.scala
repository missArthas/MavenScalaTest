package com.missArthas.scalaTest.tutorial9

import java.io.PrintWriter

import scala.io.Source
/**
  * Created by shhuang on 2017/3/28.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val source=Source.fromFile("data/test.txt","UTF-8")
    val lineIterator=source.getLines
    val out=new PrintWriter("data/testChars.txt")
    for(l<-source)
      out.println(l)
    out.close()
    source.close()

//    println("How old are u?")
//    val age=readInt()
//    println("age is "+age)

    val source1=Source.fromURL("http://www.baidu.com","UTF-8")
    val source2=Source.fromString("hello,world")
    println("source1:"+source1)
    println("source2:"+source2)
  }
}

