package scalaTest.tutorial10

/**
  * Created by shhuang on 2017/3/30.
  */
trait TimestampLogger extends Logger{
  override def log(msg: String): Unit = {
//    super.log(new java.util.Date()+" "+msg)
  }
}
