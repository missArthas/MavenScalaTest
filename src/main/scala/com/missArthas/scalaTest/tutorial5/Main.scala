package com.missArthas.scalaTest.tutorial5

/**
  * Created by shhuang on 2017/3/23.
  */
object Main {
  def main(args: Array[String]): Unit = {
    //exe1
    val counterExe1=new CounterExe1
    println(counterExe1.increment())

    //exe2
    val bankAccountExe2=new BankAccountExe2
    println(bankAccountExe2.deposit(100))
    println(bankAccountExe2.withdraw(50))

    //exe3
    val time1=new TimeExe3(1,2)
    val time2=new TimeExe3(2,2)
    println(time1.before(time2))
  }
}
