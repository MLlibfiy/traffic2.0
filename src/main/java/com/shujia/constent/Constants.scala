package com.shujia.constent

import com.shujia.common.Config

/**
  * 常量类
  *
  */
object Constants {


  val ZOOKEEPER = Config.getString("zookeeper")

  /**
    * kafka数据分隔符
    */
  val KAFKA_IN_SPLIT = "\t"

  val IN_SPLIT = ","
  val OUT_SPLIT = ","
  /**
    * spark home路径
    */
  val SPARK_HOME = Config.getString("spark.home")

  /**
    * 项目名
    */
  val PROJECT_NAME = Config.getString("project.name")


  val KAFKA_BROKER_LIST = Config.getString("kafka.metadata.broker.list")

  val TOPICS = Config.getString("topics")


  val JDBC_DRIVER = Config.getString("jdbc.driver")
  val JDBC_DATASOURCE_SIZE = Config.getInt("jdbc.datasource.size")
  val JDBC_URL = Config.getString("jdbc.url")
  val JDBC_USER = Config.getString("jdbc.user")
  val JDBC_PASSWORD = Config.getString("jdbc.password")


  val CAR_FLOW_OUT_PUT_PATH = Config.getString("car_flow_out_put_path")
  val CAR_FLOW_OUT_PUT_PATH_TMP = Config.getString("car_flow_out_put_path_tmp")

  val CAR_FLOW_CHECKPOINT = Config.getString("car_flow_checkpoint")
}
