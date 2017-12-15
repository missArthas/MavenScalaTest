package com.missArthas.test

object TupleTest {
  def main(args: Array[String]): Unit = {
    val t = (1, 'a', "test", true)

    val t2 = new Tuple2(1, 'a')

    println(t.toString())
    t.productIterator.foreach(println)
    println(t2.swap)
  }
}
