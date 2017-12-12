package com.missArthas.scalaTest.tutorial5

/**
  * Created by shhuang on 2017/3/23.
  */
class BankAccountExe2 {
  var balance=0.0

  def deposit(x:Double):Double={
    balance+=x
    balance
  }

  def withdraw(x:Double):Double={
    if(balance-x>=0) balance-=x
    balance
  }

}
