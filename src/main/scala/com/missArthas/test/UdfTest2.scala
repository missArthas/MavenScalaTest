package com.missArthas.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by nali on 2017/12/14.
  */
object UdfTest2 {
    Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    implicit val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val table = sc.parallelize(
      Seq(("alice", "ip1"), ("lina", "ip2"), ("seven", "ip3"), ("alice", "ip2"), ("seven", "ip2"), ("alice", "ip4")))
      .toDF("ID", "loginIP")
    table.registerTempTable("table")

    sqlContext.udf.register("list_size", (s: String) => s.split(',').size)

    /*
    val sql =
      """select ID,ip_list,list_size(ip_list) as loginIP_num
        |from (select ID,concat_ws(',',collect_set(loginIP)) as ip_list from table)
      """.stripMargin
    */
    //val sql = "select t.ID, t.loginIP from table t group by t.ID"

    val sql =
      """select ID,ip_list,list_size(ip_list) as loginIP_num
        |from (select ID,concat_ws(',',collect_set(loginIP)) as ip_list from table)
      """.stripMargin

    val sql2 = """select ID, ip_list,list_size(ip_list) as loginIP_num from
              |(select ID,concat_ws(',', collect_set(loginIP)) as ip_list from table group by ID)
               """.stripMargin
    sqlContext.sql(sql2).show()

  }

}
