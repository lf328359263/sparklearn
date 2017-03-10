package com.kafka_hdfs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaToHDFS {
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("kafka-hdfs")
  //    .setMaster("local[2]")
  val ssc = new StreamingContext(conf, Minutes(1))
  val topic = Map("http_log_1" -> 1)
  val zkQuorum = "hdp-1:2181,hdp-2:2181,hdp-3:2181"
  val lines = KafkaUtils.createStream(ssc, zkQuorum, "test_001", topic, StorageLevel.MEMORY_AND_DISK)
  val l2 = lines.map(_._2)
  l2.repartition(1)
    .saveAsTextFiles("/user/test/kafka_hdfs", "2017-01-01")
  ssc.start()
  ssc.awaitTermination()
}
