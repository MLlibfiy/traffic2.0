package com.shujia.index

import com.shujia.common.SparkTool
import com.shujia.constent.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnectionManager, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext


/**
  * 计算出每一个区域top3的道路流量
  *
  */
object AreaTop3RoadFlowAnalyze extends SparkTool {
  /**
    * spark配置初始化方法，初始化conf对象
    */
  override def init(args: Array[String]): Unit = {
  }

  /**
    * spark主逻辑方法
    * 该方法内不能配置conf
    *
    * @param args
    */
  override def run(args: Array[String]): Unit = {

    /**
      * 需要分析的数据分区
      */
    val time = args(0)

    val hiveContext = new HiveContext(sc)

    val sql = "select area_id,road_id,c from (select area_id,road_id,c,row_number() over(partition by area_id order by c desc) as rank from  (select area_id,road_id,count(1) as c from car_flow where time=" + time + " group by area_id,road_id) as a )as b where rank<=3;"

    val df = hiveContext.sql(sql)


    df.rdd.foreachPartition(rowList => {
      //建立hbase连接
      val conf: Configuration = new Configuration
      conf.set("hbase.zookeeper.quorum", Constants.ZOOKEEPER)
      //创建zookeeper连接
      val connection = HConnectionManager.createConnection(conf)
      val table = connection.getTable("AreaTop3RoadFlowAnalyze")

      /**
        * create 'AreaTop3RoadFlowAnalyze', {NAME => 'info', VERSIONS => 1}
        */

      for (row <- rowList) {
        val areaId = row.getAs[String]("area_id")
        val roadId = row.getAs[String]("road_id")
        val count = row.getAs[String]("c")
        val rowkey = areaId + "_" + time + "" + roadId
        val put = new Put(rowkey.getBytes())
        put.add("info".getBytes(),"count".getBytes(),Bytes.toBytes(count.toInt))

        table.put(put)
      }

      table.close()
      connection.close()
    })
  }
}
