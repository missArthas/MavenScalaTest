package com.missArthas.test

import java.sql.Time

object DateStampTest {
  def main(args: Array[String]): Unit = {
    /**device id: 00000000-7a8f-38e6-ffff-ffffec7ed2ef*/
    println("展示1：", new Time(1513656016216l))
    println("展示2:", new Time(1513650102364l))
    println("浏览:", new Time(1513650126556l))

    /**device id: ffffffff-e09f-77d4-0033-c587003652ee*/
    println("展示1：", new Time(1513650103615l))
    println("展示2:", new Time(1513650337201l))
    print("浏览:", new Time(1513650128138l))
  }
}
