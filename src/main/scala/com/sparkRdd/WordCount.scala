package com.sparkRdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分词统计前30
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("/user/hive/warehouse/mytest.db/graphic/demo.txt")
    file.cache()
    file.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _).map(w => (w._2, w._1)).sortByKey(false).map(w => (w._2, w._1)).take(30).foreach(println)

  }
}
