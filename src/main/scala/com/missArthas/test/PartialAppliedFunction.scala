package com.missArthas.test

import java.util.Date

object PartialAppliedFunction {
  def main(args: Array[String]): Unit = {
    println("tradition function:")
    val date = new Date()
    func("message1", date)
    func("message2", date)
    func("message3", date)
    println()

    println("partial apply funcion:")
    val partialFunc = func(_: String, date)
    partialFunc.apply("p1")
    partialFunc.apply("p2")
    partialFunc.apply("p3")
  }

  def func(s: String, d: Date): Unit ={
    println(s+" "+d)
  }
}
