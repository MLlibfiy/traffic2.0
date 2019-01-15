package com.shujia.common

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 作为传递类参数的类
  * spark main方法类实现接口
  * @author qinxiao
  */
trait SparkTool {
  var conf: SparkConf = _
  var sc: SparkContext = _
  var LOGGER: Logger = _

  def main(args: Array[String]): Unit = {
    //日志对象
    LOGGER = LoggerFactory.getLogger(this.getClass)
    try {
      //加载用户自定义配置文件,集群运行起作用
      Config.loadCustomConfig(Config.getDefaultCustomConfigPath)
    }catch {
      case e: Exception=>
    }
    conf = new SparkConf()
    //设置spark appName
    conf.setAppName(this.getClass.getSimpleName.replace("$", ""))
    this.init(args)
    sc = new SparkContext(conf)
    LOGGER.info("开始执行spark任务")
    this.run(args)
    LOGGER.info("spark任务执行完成")
  }

  /**
    * spark配置初始化方法，初始化conf对象
    */
  def init(args: Array[String])

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    * @param args
    */
  def run(args: Array[String])


}