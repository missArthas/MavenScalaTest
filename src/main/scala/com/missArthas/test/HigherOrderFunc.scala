package com.missArthas.test

object HigherOrderFunc {
    def main(args: Array[String]) {

      println( apply( layout2, 10) )

    }
    // 函数 f 和 值 v 作为参数，而函数 f 又调用了参数 v
    def apply(f: Int => String, v: Int) = f(v)

    def layout[A](x: A) = "function1[" + x.toString() + "]"

    def layout2(x: Int) = "function2[" + x.toString() + "]"

}
