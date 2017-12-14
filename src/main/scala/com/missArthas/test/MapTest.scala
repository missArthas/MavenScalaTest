package com.missArthas.test

object MapTest {
  def main(args: Array[String]): Unit = {
    var mapA:Map[String, Int] = Map()

    val mapB = Map(1 -> "a", 2 -> "b", 3 -> "c")

    mapA += ("a" -> 1)
    mapA += ("b" -> 2)
    mapA += ("c" -> 3)


    println("mapA keys " + mapA.keys)
    println(mapA.get("a"))

    println("mapB keys " + mapB.keys)

    println("mapA ++ mapB " + mapA ++ mapB)
  }
}
