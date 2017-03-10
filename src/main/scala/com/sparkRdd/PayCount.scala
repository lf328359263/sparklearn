package com.sparkRdd

import org.apache.spark.{SparkConf, SparkContext}

object PayCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("/user/hive/warehouse/ty.db/user_vouch/vouch_result.txt")
    file.cache()
    file.map(line => {val words = line.split("\t"); (words(0), words(2).toDouble)}).reduceByKey(_+_).foreach(println)
  }
}
