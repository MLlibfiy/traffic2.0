package com.shujia.rtmroad

import com.shujia.common.SparkTool
import com.shujia.constent.Constants
import com.shujia.util.DateUtils
import java.util.Date

object MergeCarFlowTmpData extends SparkTool {
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

    /**
      * 获取前一分钟目录列表
      */
      val time = DateUtils.formatTimeMinute(new Date())






    val partList = List("1547709685", "1547709690")

    val cityodPath = "E:\\bigdata\\traffic\\data\\a={1,2}"
    sc.textFile(cityodPath).foreach(println)
  }
}
