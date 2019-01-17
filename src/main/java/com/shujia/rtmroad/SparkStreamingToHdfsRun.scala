package com.shujia.rtmroad

import com.shujia.common.SparkRunner

object SparkStreamingToHdfsRun {
  def main(args: Array[String]): Unit = {
    SparkRunner.run(SparkStreamingToHdfs, List(("", "")), args)
  }
}
