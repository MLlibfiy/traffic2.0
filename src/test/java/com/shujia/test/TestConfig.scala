package com.shujia.test

import com.shujia.common.Config
import com.shujia.util.DateUtils
import org.junit.Test
import java.util.Date

class TestConfig {

  @Test
  def test(): Unit ={
   println(DateUtils.formatTimeMinute(new Date()))
  }

}
