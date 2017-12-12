package com.missArthas.scalaTest.tutorial2

/**
  * Created by shhuang on 2017/3/14.
  */
object AnyTest {
  def main(args: Array[String]): Unit = {
    val list:List[Any]=List(
      "hello",
      1,
      true,
      ()=>println("function")
    )
    print(list)
    list.foreach(a=>println(a))
  }
}
