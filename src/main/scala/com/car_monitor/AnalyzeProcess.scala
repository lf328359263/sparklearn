package com.car_monitor

import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

/**
  * Created by root on 2017-03-12.
  */
object AnalyzeProcess {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    val appName = "Car_Analyze"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(5))
    //    ssc.checkpoint(".")

    // Kafka configurations
    val topics = Set(MyConfig.topic)
    val brokers = MyConfig.brokers

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers)


    val dbIndex = 1

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //    val kafkaStream = KafkaUtils.createDirectStream(ssc, kafkaParams, fromOffsets, messageHandler)

    val events = kafkaStream.flatMap(line => {
      val data = new JSONObject(line._2)
//      val data = JSONObject.fromObject(line._2)
      println(data)
      Some(data)
    })
    //            .filter(x => (x.getString("car_id").matches("'[0-9A-Z].*")))

    // Compute car average speed for every camera

    //    val dateString = (input:String) => {input.substring(1,14)}
    //    dateString(x.getString("event_time")) + "_" + x.getString("road_id")
    //    val ff = (a:Tuple2[Int,Int], b:Tuple2[Int,Int]) => {(a._1 + b._1, a._2 + b._2)};
    val carSpeed = events.map(x => (x.getString("camera_id"),x.getInt("speed")))
      //    val carSpeed = events.map(x => x.getString("road_id") -> (x.getInt("speed"),1))
      //            .reduceByKey((a, b) => {(a._1 + b._1, a._2 + b._2)})
      .mapValues((x:Int)=>(x,1.toInt))
      //            .reduceByKeyAndWindow((a, b) => {(a._1 + b._1, a._2 + b._2)},Seconds(10))

      .reduceByKeyAndWindow((a:Tuple2[Int,Int], b:Tuple2[Int,Int]) => {(a._1 + b._1, a._2 + b._2)},Seconds(20),Seconds(10))

    //    carSpeed.map{ case (key, value) => (key, value._1 / value._2.toFloat) }

    carSpeed.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        val jedis = RedisClient.pool.getResource
        partitionOfRecords.foreach(pair => {
          val camera_id = pair._1
          val total = pair._2._1
          val count = pair._2._2
          val now = Calendar.getInstance().getTime()
          // create the date/time formatters
          val minuteFormat = new SimpleDateFormat("HHmm")
          val dayFormat = new SimpleDateFormat("yyyyMMdd")
          val time = minuteFormat.format(now)
          val day = dayFormat.format(now)
          if(count!=0){
            //            val averageSpeed = total / count
            jedis.select(dbIndex)
            jedis.hset(day + "_" + camera_id, time , total + "_" + count)
            // fetch data from redis
            //            val temp = jedis.hget(day + "_" + camera_id, time)
            //            println(temp)
          }
        })
        RedisClient.pool.returnResource(jedis)
      })

    })

    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    ssc.start()
    ssc.awaitTermination()

  }
}
