package scalaTest.tutorial4AnonymousFunctions

/**
  * Created by shhuang on 2017/3/14.
  */
object AnonymousFunction {
  def main(args: Array[String]): Unit = {
    val plusOne=(x:Int)=>x+1
    val returnString=()=>"hello,world"
    println(plusOne(2))
    println(returnString())
  }

}
