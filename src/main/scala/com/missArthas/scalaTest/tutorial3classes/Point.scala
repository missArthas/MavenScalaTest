package com.missArthas.scalaTest.tutorial3classes

/**
  * Created by shhuang on 2017/3/14.
  */
class Point(var x:Int,var y:Int) {
  override def toString: String = ("(" + x + "," + y + ")")

  def move(a: Int, b: Int): Unit = {
    x = x + a
    y = y + b
  }
}
