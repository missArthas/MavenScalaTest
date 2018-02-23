package com.missArthas.test

import java.util.Calendar

object CalendarTest {
  def main(args: Array[String]): Unit = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    println(new java.text.SimpleDateFormat("yyyy/MM/dd").format(cal.getTime))
  }
}
