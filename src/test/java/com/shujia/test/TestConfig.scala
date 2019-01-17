package com.shujia.test

import com.shujia.common.Config
import com.shujia.util.DateUtils
import org.junit.Test
import java.util.Date

class TestConfig {

  @Test
  def test(): Unit = {
    val time = DateUtils.formatTimeMinute(new Date())
    println(time)
    println(DateUtils.getYestoMinute("201901171600"))
  }

}
