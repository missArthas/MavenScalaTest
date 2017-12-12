package com.missArthas.scalaTest.tutorial1

/**
  * Created by shhuang on 2017/3/14.
  */
class Great(prefix:String,suffix:String) {
  def great(content:String):Unit={
    println(prefix+" "+content+" "+suffix)
  }
}
