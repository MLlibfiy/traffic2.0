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
}
