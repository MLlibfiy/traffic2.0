package com.shujia.test

import org.junit.Test

class TestConfig {

  @Test
  def test(): Unit ={
    val i = com.shujia.common.Config.getInt("test")
    println(i)
  }

}
