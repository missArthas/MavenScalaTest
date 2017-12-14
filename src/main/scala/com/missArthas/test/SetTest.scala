package com.missArthas.test

object SetTest {
  def main(args: Array[String]): Unit = {
    val set1 = Set(1,2,3)
    val set2 = Set("one", "two", "three")

    println(set1.head)
    println(set1.tail)

    println(set1 ++ set2)
  }
}
