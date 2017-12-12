package com.missArthas.scalaTest.tutorial2

/**
  * Created by shhuang on 2017/3/14.
  */
object Exercise {
  def main(args: Array[String]): Unit = {
    (1 to 10).reverse.foreach(t=>println(t))
    countdown(3)
    println(cal2("Hello"))
    println(recCal("Hello"))
    println(e10(5,-1))
  }

  def countdown(n:Int): Unit ={
    (n to 0).foreach(t=>println(t))
  }

  def cal(str:String):Long={
    var sum:Long=1
    for(c<-str){
      sum*=c.toInt
    }
    sum
  }

  def cal2(str:String):Long={
    var sum:Long=1
    str.foreach(c=>sum*=c.toLong)
    sum
  }

  def recCal(str:String):Long={
    if(str.length==1) str.head.toLong
    else str.head.toLong*recCal(str.tail)
  }

  def e10(x:Double,n:Int):Double={
    if(n==0) 1
    else if (n==2) x*x
    else if (n%2==1) x*e10(x,n-1)
    else if (n%2==0) e10(e10(x,n/2),2)
    else 1/e10(x,-n)

  }

}
