package com.car_monitor

/**
  * Created by root on 2017-03-12.
  */
object MyConfig {
  val brokers = "hdp-1:9092,hdp-2:9092,hdp-3:9092"
  val redisHost = "hdp-1"
  val redisPort = 6379
  val topic = "car_events"
  val filePath = "/user/2014082013_all_column_test.txt"
  val key_serializer = "org.apache.kafka.common.serialization.StringSerializer"
  val value_serializer = "org.apache.kafka.common.serialization.StringSerializer"
}
