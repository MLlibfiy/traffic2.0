package com.shujia.test

import java.text.SimpleDateFormat

import com.shujia.common.Config
import com.shujia.util.DateUtils
import org.junit.Test
import java.util.Date

class TestConfig {

  @Test
  def test(): Unit = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

   println( sdf.parse("2019-01-18T15:51:24.283+08:00".replace("T"," ")))


  }

}
