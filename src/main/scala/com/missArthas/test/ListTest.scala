package com.missArthas.test

object ListTest {
  def main(args: Array[String]): Unit = {
    val siteList1 = "baidu" :: ("google" :: ("yahoo" :: Nil))
    val siteList2 = "alibaba" :: "tencent" :: "netease" :: Nil

    println(siteList1.head)
    println(siteList1.tail)
    println(siteList1.isEmpty)

    println("list1 ::: list2", siteList1 ::: siteList2)
    println("list.:::list2", siteList1.:::(siteList2))
    println("list concat list2", List.concat(siteList1, siteList2))
  }
}
