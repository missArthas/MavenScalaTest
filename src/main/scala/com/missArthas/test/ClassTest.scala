package com.missArthas.test

// 私有构造方法
class ClassTest private(val color:String) {

  println("创建" + this)

  override def toString(): String = " 颜色标记："+ color

}

// 伴生对象，与类共享名字，可以访问类的私有属性和方法
object ClassTest{

  private val markers: Map[String, ClassTest] = Map(
    "red" -> new ClassTest("red"),
    "blue" -> new ClassTest("blue"),
    "green" -> new ClassTest("green")
  )

  def apply(color:String) = {
    if(markers.contains(color)) markers(color) else null
  }


  def getMarker(color:String) = {
    if(markers.contains(color)) markers(color) else null
  }
  def main(args: Array[String]) {
    println(ClassTest("red"))
    // 单例函数调用，省略了.(点)符号
    println(ClassTest getMarker "blue")
  }
}
