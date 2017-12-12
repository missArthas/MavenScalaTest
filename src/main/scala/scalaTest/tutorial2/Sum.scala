package scalaTest.tutorial2

/**
  * Created by shhuang on 2017/3/14.
  */
object Sum {
  def main(args: Array[String]): Unit = {
    println(sum(1,2,3))
    println(recSum(1,2,3))
    println(recSum(1 to 3: _*))

    lazy val word=scala.io.Source.fromFile("data/test.txt")
    word.foreach(c=>print(c))
  }

  def sum(nums:Int*):Int={
    var result=0
    for(t<-nums) result+=t
    result
  }

  def recSum(nums:Int*):Int={
    if(nums.length==0) 0
    else nums.head+recSum(nums.tail:_*)
  }
}
