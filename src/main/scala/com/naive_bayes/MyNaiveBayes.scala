package com.naive_bayes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 2017-03-12.
  */
object MyNaiveBayes {

  def main(args: Array[String]): Unit = {
    val master = "local[2]"
    val filePath = ""
    val resultModelPath = ""
    val conf = new SparkConf().setAppName("naive_bayes").setMaster(master)
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

//    加载样本数据
    val data = sc.textFile(filePath)
    val parsedData = data.map(line => {val parts = line.split(','); LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split("_").map(_.toDouble)))})

//    划分训练样本与测试样本
    val splits = parsedData.randomSplit(Array(0.6,0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

//    贝叶斯分类  训练
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

//    测试
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction \t label")
    for(dict <- print_predict){
      println(dict._1+"\t"+dict._2)
    }

    val accuracy = 1.0 * predictionAndLabel.filter(x=>x._1 == x._2).count() / test.count()

//    保存
    model.save(sc, resultModelPath)
//    加载已有模型
    val sameModel = NaiveBayesModel.load(sc, resultModelPath)

  }

}
