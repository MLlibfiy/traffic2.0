package com.shujia.rtmroad


import java.text.SimpleDateFormat

import com.shujia.common.SparkTool
import com.shujia.constent.Constants
import com.shujia.util.DateUtils
import java.util.{Calendar, Date}

import com.shujia.common.boot.HadoopUtil
import org.apache.hadoop.fs.{FileSystem, Path}

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
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr = args(0)
    val date = sdf.parse(dateStr.replace("T"," "))
    val time = DateUtils.formatTimeMinute(date)
    val lastTIme = DateUtils.getYestoMinute(time)

    //输入数据路径
    val inputPath = Constants.CAR_FLOW_OUT_PUT_PATH_TMP + "/time=" + lastTIme + "*"

    val RDD1 = sc.textFile(inputPath)

    val outPutPath = Constants.CAR_FLOW_OUT_PUT_PATH+"/time="+lastTIme

    //删除输出目录
    val fileSystem = FileSystem.get(HadoopUtil.getHadoopConfig)
    if (fileSystem.exists(new Path(outPutPath))) {
      //删除输出目录
      fileSystem.delete(new Path(outPutPath), true)
    }

    /**
      * 数据存到hdfs
      */
    RDD1
      .coalesce(6, false)
      .saveAsTextFile(outPutPath)



  }
}
