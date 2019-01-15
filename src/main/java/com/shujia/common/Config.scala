package com.shujia.common

import java.io.InputStream
import java.util.Properties

object Config {

  val properties = new Properties()
  val in: InputStream = Config.getClass.getClassLoader.getResourceAsStream("config.properties")
  properties.load(in)

  //获取指定key对应的value
  def getString(key: String): String = {
    properties.getProperty(key)
  }

  //获取整数类型的配置项
  def getInteger(key: String): Integer = {
    var value = getString(key)
    try {
      return Integer.valueOf(value)
    } catch {
      case e: Exception => println(e)
    }
    0
  }

  //获取布尔类型的配置项
  def getBoolean(key: String): Boolean = {
    val value: String = getString(key)
    try {
      return value.toBoolean
    } catch {
      case e: Exception => println(e)
    }
    false
  }

  //获取Long类型的配置项
  def getLong(key: String): Long = {
    val value = getProperty(key)
    try
      return value.toLong
    catch {
      case e: Exception => println(e)
    }
    0
  }
}
