package com.missArthas.scalaTest.tutorial5

/**
  * Created by shhuang on 2017/3/23.
  */
class TimeExe3(val hrs:Int,val min:Int) {

  def before(other:TimeExe3):Boolean={
    if(this.hrs<other.hrs) true
    else if(this.hrs==other.hrs&&this.min<other.min) true
    else false
  }

}
