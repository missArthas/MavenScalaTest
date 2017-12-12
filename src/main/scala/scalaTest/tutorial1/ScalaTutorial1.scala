package scalaTest.tutorial1

/**
  * Created by shhuang on 2017/3/14.
  */
object ScalaTutorial1 {
  def main(args: Array[String]): Unit = {
    //functions and block
    val addOne=(x:Int)=>{
      val t=x+1
      t*2
    }
    println(addOne(2))

    //no parameter
    val printSquare=()=>{
      val t=2
      t*t
    }
    println(printSquare())

    println(add(1,2))
    val nums:Seq[Int]=Seq(1,2,3)
    println(addMultipleNumber(nums))

    val a=new Great("hello","world")
    a.great("kevin")

    val point1=new Point(1,2)
    val point2=new Point(1,2)
    val point3=new Point(2,2)
    println(point1==point2)
    println(point1==point3)
  }

  //methods
  def add(x:Int,y:Int):Int={
  x*y
  }

  def addMultipleNumber(x:Seq[Int]):Int={
    val a=x
    x.reduce((a:Int,b:Int)=>a+b)
  }
}
