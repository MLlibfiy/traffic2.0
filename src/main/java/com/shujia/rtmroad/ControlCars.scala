package com.shujia.rtmroad

import java.util

import com.shujia.common.{Config, SparkTool}
import com.shujia.constent.Constants
import com.shujia.util.JDBCHelper
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * 缉查布控
  *
  * 1、通过spark streaming 连接kafka读取数据车流量数据
  * 2、从数据库查询缉查布控的车牌号
  * 3、过滤出现的车辆
  * 4、将结果存到数据库
  *
  * @param args
  */
object ControlCars extends SparkTool {
  /**
    * spark配置初始化方法，初始化conf对象
    */
  override def init(args: Array[String]): Unit = {
    conf.setMaster("local")

  }

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {
    //构建spark streaming 上下文对象
    val ssc = new StreamingContext(sc, Durations.seconds(5))

    /**
      * 连接kafka
      * 使用direct模式
      *
      */
    val flowDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map("metadata.broker.list" -> Constants.KAFKA_BROKER_LIST),
      Constants.TOPICS.split(",").toSet
    )


    /**
      * 缉查布控
      */


    /**
      * 动态修改广播变量
      *
      */
    flowDS.map(_._2).foreachRDD(rdd=>{
      val context = rdd.context
      /**
        * 读取mysql,查询需要缉查布控的车辆
        */

      val sqlContext = new SQLContext(context)
      val arsDF = sqlContext.read.format("jdbc").options(
        Map(
          "url" -> Constants.JDBC_URL,
          "driver" -> Constants.JDBC_DRIVER,
          "dbtable" -> "traffic.control_cars",
          "user" -> Constants.JDBC_USER,
          "password" -> Constants.JDBC_PASSWORD
        )).load()

      val carlist = arsDF.select("car").rdd.collect().toList

      //每一次batch都会广播一次
      val carlistbroadcast = sc.broadcast(carlist)


      LOGGER.info("布控的车辆："+carlist.mkString(","))

      val controlcarRDD =  rdd.filter(l =>{
        LOGGER.info(l)
        val cars = carlistbroadcast.value
        val car = l.split("\t")(3)
        cars.contains(car)
      })


      /**
        * 将结果存到数据亏
        *
        */
      controlcarRDD.foreachPartition(i=>{
        /**
          * 获取数据库连接
          */


        val sql = "INSERT INTO TABLE control_flow VALUES(?,?,?,?,?,?,?,?)"

        i.foreach(line=>{
          val list = new util.ArrayList[Array[String]]()
          list.add(line.split("\t"))
          JDBCHelper.getInstance().executeBatch(sql,list)
        })


      })

    })


    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
