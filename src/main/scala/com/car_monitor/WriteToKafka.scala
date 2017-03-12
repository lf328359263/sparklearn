package com.car_monitor

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject


object WriteToKafka {
  def main(args: Array[String]): Unit = {
    //    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    val topic = MyConfig.topic
    val master = "local[4]"
    val appName = "traffic_spout"
    val filePath = MyConfig.filePath
    val brokers = MyConfig.brokers
    val props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("batch.size", "5")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)

      val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
      val sc = new SparkContext(sparkConf)
//      sc.
      val records = sc.textFile(filePath).filter(!_.startsWith(";")).map(_.replace("'","").split(",")).collect()

      for (record <- records){
        val event = new JSONObject()
        event.put("camera_id", record(0))
        .put("car_id", record(2))
        .put("event_time", record(4))
        .put("speed", record(6))
        .put("road_id", record(13))
      producer.send(new ProducerRecord[String, String](topic, event.toString))
      println(event.toString)
      Thread.sleep(200)
    }

  }
}
