package com.shujia.constent

import com.shujia.common.Config

/**
  * 常量类
  *
  */
object Constants {

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
}
