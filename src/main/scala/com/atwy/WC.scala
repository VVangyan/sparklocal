package com.atwy

import org.apache.spark.{SparkConf, SparkContext}

object WC {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setAppName("WC").setMaster("local")

    val stx = new SparkContext(config)

    val fileRdd = stx.textFile("C://Users//yan//Desktop//大数据相关//sparkfile//spark.txt")

    val wordTupe = fileRdd.flatMap(line => line.split(" "))

    wordTupe.map((_, 1)).reduceByKey(_ + _).saveAsTextFile("C://Users//yan//Desktop//大数据相关//sparkfile//result.txt")

    println("处理完成。。。")

    stx.stop()
  }

}
