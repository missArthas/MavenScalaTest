package com.missArthas.test

object ForLoopTest {
  def main(args: Array[String]): Unit = {
    val numList = List(1,2,3,4,5,6,7,8,9,10)

    for( a <- numList){
      println(a)
    }

    val t = for{ a <- numList
      if( a % 2 == 0)
    }yield  a

    println(t)
  }
}
