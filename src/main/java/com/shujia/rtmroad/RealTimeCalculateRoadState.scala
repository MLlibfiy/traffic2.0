package com.shujia.rtmroad

import com.shujia.common.SparkTool
import com.shujia.constent.Constants
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 计算道路实时溶度情况
  * 需求：每秒算一次，计算最近30秒道路的平均速度
  * 需求拆分：窗口大小=30秒，滑动时间=1秒，batchi= 1秒（一秒拉取一次数据）
  *
  * 1、读取kafka数据，
  * 2、获取道路编号，车辆速度
  * 3、计算道路平均速度，总的车辆数，总的速度
  * 4、将结算结果保存到hbase
  * 1、以道路编号作为rowkey
  * 2、均速度，总的车辆数，总的速度 ，保存三列（版本数量根据实际需求设置）
  *
  */
object RealTimeCalculateRoadState extends SparkTool {
  /**
    * spark配置初始化方法，初始化conf对象
    */
  override def init(args: Array[String]): Unit = {
    conf.setMaster("local[4]")
  }

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc, Durations.seconds(1))


    val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
      Constants.ZOOKEEPER, //zookeeper 地址
      "RealTimeCalculateRoadState", //消费者组 ，随便取个名字就就可以
      Map(Constants.TOPICS -> 1),
      StorageLevel.MEMORY_AND_DISK_SER) //数据拉去过来之后的持久化级别


    /**
      * 计算总速度和总车辆数
      */
    val count = (x: (Int, Int), y: (Int, Int)) => {
      //计算总速度
      val sumSpeed = x._1 + y._1
      //计算总车辆数
      val sumNum = x._2 + y._2
      (sumSpeed, sumNum)
    }

    kafkaDS
      .map(_._2.split(Constants.KAFKA_IN_SPLIT)) //切分数据
      .map(line => (line(6), (line(5).toInt, 1))) //取出道路编号和速度
      .reduceByKeyAndWindow(count, Durations.seconds(30), Durations.seconds(1))
      .map(t => {
        val roadId = t._1
        //总速度
        val sumSpeed = t._2._1
        //总车辆数
        val sumNum = t._2._2
        //计算平均速度
        val avgSpeed = sumSpeed / sumNum
        (roadId, sumSpeed, sumNum, avgSpeed)
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
