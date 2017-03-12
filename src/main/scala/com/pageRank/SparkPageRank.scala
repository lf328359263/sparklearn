package com.pageRank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkPageRank {
  def main(args: Array[String]) {
    //    if (args.length < 1) {
    //      System.err.println("Usage: SparkPageRank <file> <iter>")
    //      System.exit(1)
    //    }
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.streaming").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("PageRank")
      .setMaster("local[1]")
    val iters = 200;
    //    val iters = if (args.length > 0) args(1).toInt else 10
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile("page.txt", 1)

    //根据边关系数据生成 邻接表 如：(1,(2,3,4,5)) (2,(1,5))..
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    links.foreach(x => {
      print("xcvzx"); println(x)
    })

    // (1,1.0) (2,1.0)..
    var ranks = links.mapValues(v => 1.0)

    ranks.foreach(println)

    for (i <- 1 to iters) {
      // (1,((2,3,4,5), 1.0))
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

    ctx.stop()
  }
}