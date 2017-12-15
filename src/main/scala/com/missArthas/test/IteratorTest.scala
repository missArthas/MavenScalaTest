package com.missArthas.test

object IteratorTest {
  def main(args: Array[String]): Unit = {
    val t = Iterator(1, 2, 3)
    //println(t.toString())

    //println(t.max)

    while(t.hasNext){
      println(t.next())
    }


  }
}
