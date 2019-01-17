package com.shujia.rtmroad

import com.shujia.common.SparkTool
import com.shujia.constent.Constants
import com.shujia.util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Date

object SparkStreamingToHdfs extends SparkTool{
  /**
    * spark配置初始化方法，初始化conf对象
    */
  override def init(args: Array[String]): Unit = {

  }

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    ssc.checkpoint(Constants.CAR_FLOW_CHECKPOINT)

    val carDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map("metadata.broker.list" -> Constants.KAFKA_BROKER_LIST),
      Constants.TOPICS.split(",").toSet
    )

    /**
      * DS ---> RDD
      *
      */
    carDS.map(_._2)
      .filter(line => true)//过滤掉脏数据
      .foreachRDD(rdd => {

      val time = System.currentTimeMillis().toString.substring(0,10)

      //数据存到hdfs  ,每5秒一个分区，一般来说，需要对小文件合并
      rdd.saveAsTextFile(Constants.CAR_FLOW_OUT_PUT_PATH_TMP +"/time="+DateUtils.formatTimeMillis(new Date()))

    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
