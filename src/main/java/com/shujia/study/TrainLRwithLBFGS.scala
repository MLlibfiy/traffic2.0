package com.shujia.study

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

object TrainLRwithLBFGS {

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("Beijing traffic")
    .setMaster("local")
  val sc = new SparkContext(sparkConf)

  // create the date/time formatters
  val dayFormat = new SimpleDateFormat("yyyyMMdd")
  val minuteFormat = new SimpleDateFormat("HHmm")

  def main(args: Array[String]) {

    // fetch data from redis
    val jedis: Jedis = RedisClient.pool.getResource
    jedis.select(3)

    // find relative road monitors for specified road
    // val camera_ids = List("310999003001","310999003102","310999000106","310999000205","310999007204")

    val camera_ids = List("310999003001", "310999003102")
    val camera_relations: Map[String, Array[String]] =
      Map[String, Array[String]](
      "310999003001" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204"),
      "310999003102" -> Array("310999003001", "310999003102", "310999000106", "310999000205", "310999007204")
    )

     camera_ids.map({ camera_id =>
      val hours = 6
      //拿到当前时间戳
      val nowtimelong: Long = System.currentTimeMillis()
      val now = new Date(nowtimelong)
      //            val day = dayFormat.format(now)
      val day = "20190225"
      // Option Some None
      val list = camera_relations(camera_id)

      val relations: Array[(String, util.Map[String, String])] = list.map({ camera_id =>
        // fetch records of one camera for three hours ago
        (camera_id, jedis.hgetAll(day + "_" + camera_id))

      })
      // organize above records per minute to train data set format (MLUtils.loadLibSVMFile)
      val trainSet: ArrayBuffer[LabeledPoint] = ArrayBuffer[LabeledPoint]()
      //往训练集里面加上每一行数据，往前三小时，每分钟数减1
      for (i <- Range(60 * hours - 3, 0, -1)) {

        val featuresX: ArrayBuffer[Double] = ArrayBuffer[Double]()
        val featureY: ArrayBuffer[Double] = ArrayBuffer[Double]()
        //填三分钟的数据
        for (index <- 0 to 2) {

          val tempOne: Long = nowtimelong - 60 * 1000 * (i - index)
          val d = new Date(tempOne)
          val tempMinute: String = minuteFormat.format(d)
          val tempNext: Long = tempOne - 60 * 1000 * (-1)
          val dNext = new Date(tempNext)
          val tempMinuteNext: String = minuteFormat.format(dNext)
          //把自己和相邻路段填入值
          for ((k, v) <- relations) {
            // k->camera_id ; v->HashMap
            val map: util.Map[String, String] = v
            //填入y坐标值
            if (index == 2 && k == camera_id) {
              if (map.containsKey(tempMinuteNext)) {
                val info: Array[String] = map.get(tempMinuteNext).split("_")
                val f: Float = info(0).toFloat / info(1).toFloat
                featureY += f
              }
            }
            //填入X坐标值
            if (map.containsKey(tempMinute)) {
              val info: Array[String] = map.get(tempMinute).split("_")
              val f: Float = info(0).toFloat / info(1).toFloat
              featuresX += f
            } else {
              featuresX += -1.0
            }
          }
        }
        //判断y坐标有没有值
        if (featureY.toArray.length == 1) {
          val label: Double = featureY.toArray.head
          val record = LabeledPoint(if ((label.toInt / 10) < 10) label.toInt / 10 else 10.0, Vectors.dense(featuresX.toArray))
          //                    println(record)
          trainSet += record
        }
      }
      trainSet.foreach(println)

      //z转换成RDD
      val data: RDD[LabeledPoint] = sc.parallelize(trainSet)

      // Split data into training (60%) and test (40%).
      val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed = 1000L)
      val training = splits(0)
      val test = splits(1)

      println(data)

      if (!data.isEmpty()) {

        // 构建逻辑回归算法模型
        val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
          //设置逻辑回归分类的数量
          .setNumClasses(11)
          //训练模型
          .run(training)

        // Compute raw scores on the test set.
        val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
          val prediction: Double = model.predict(features)
          (prediction, label)
        }

        predictionAndLabels.foreach(x => println(x))

        // Get evaluation metrics.
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val precision: Double = metrics.precision
        println("Precision = " + precision)

        //判断准确率是否大于0.8
        if (precision > 0.8) {
          val path: String = "E:\\bigdata\\traffic\\out\\model_" + camera_id + "_" + nowtimelong
          model.save(sc, path)
          println("saved model to " + path)
          jedis.hset("model", camera_id, path)
        }

      }
    })

    RedisClient.pool.returnResource(jedis)
  }
}
