package com.shujia.common

import java.io.File

import com.shujia.constent.Constants
import com.shujia.util.ComandUtil
import org.slf4j.LoggerFactory

/**
  *
  * spark提交任务工具
  * @author qinxiao
  */
object SparkRunner {

  private val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  val sprkShell = new StringBuffer()

  def run(sparkTool: SparkTool, conf: List[(String, String)], args: Array[String]): Unit = {

    //加载用户自定义配置文件,集群运行起作用
    Config.loadCustomConfig(Config.getDefaultCustomConfigPath)

    val sparkHome = Constants.SPARK_HOME
    if ("".equals(sparkHome)) {
      LOGGER.warn("请在default.properties中配置spark.home")
      sprkShell.append("spark-submit ")
    } else {
      sprkShell.append(sparkHome).append("/spark-submit ")
    }
    //增加spark参数
    addConf(conf)
    //加载jar包
    loadOtherJars()
    //增加main方法所在类和main方法所在jar包
    addMainClassAndMainJar(sparkTool)
    //增加参数
    args.foreach(sprkShell.append(" ").append(_))
    LOGGER.info("******************** START THE SPARK JOB ********************")
    LOGGER.info(sprkShell.toString)
    //执行spark程序
    //ComandUtil.execute(sprkShell.toString)
  }

  /**
    * 增加spark参数
    */
  def addConf(conf: List[(String, String)]): Unit = {
    LOGGER.info("******************** SPARK CONFIGURATION ********************")
    //增加spark配置
    conf.foreach(x => {
      LOGGER.info(String.format("%-30s: [%s]", x._1, x._2))
      sprkShell.append(" --")
        .append(x._1)
        .append(" ")
        .append(x._2)
        .append(" ")
    })
  }

  /**
    * 加载第三方jar包
    */
  def loadOtherJars(): Unit = {
    val listFiles = getLibJars()
    val jars = listFiles.filter(_.endsWith("jar")).mkString(",")
    //增加jar包
    sprkShell.append(" --jars ").append(jars).append(" ")
  }

  /**
    * 增加main方法所在类和main方法所在jar包
    */
  def addMainClassAndMainJar(sparkTool: SparkTool): Unit = {
    //增加类
    sprkShell.append(" --class ").append(sparkTool.getClass.getName.replace("$", ""))
    //增加main方法所在jar包
    val listFiles = getLibJars()
    val projectName = Constants.PROJECT_NAME
    if (projectName == null) {
      LOGGER.error("请在default.properties中配置project.name")
    } else {
      val mainJar = listFiles.map(_.split("/").last).filter(_.startsWith(projectName)).head
      sprkShell.append(" ../lib/").append(mainJar)
    }
  }

  /**
    * 获取lib目录下所有jar包
    *
    * @return
    */
  def getLibJars(): List[String] = {
    val listFiles = new File(new File(System.getProperty("user.dir")).getParentFile, "lib")
      .listFiles
      .toList
      .map(_.getAbsolutePath)
    listFiles
  }
}