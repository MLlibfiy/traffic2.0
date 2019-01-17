package com.shujia.constent;

import com.shujia.common.Config;

public interface Constants {

    String ZOOKEEPER = Config.getString("zookeeper");

    /**
     * kafka数据分隔符
     */
    String KAFKA_IN_SPLIT = "\t";

    String IN_SPLIT = ",";
    String OUT_SPLIT = ",";
    /**
     * spark home路径
     */
    String SPARK_HOME = Config.getString("spark.home");

    /**
     * 项目名
     */
    String PROJECT_HOME = Config.getString("project.home");


    String KAFKA_BROKER_LIST = Config.getString("kafka.metadata.broker.list");

    String TOPICS = Config.getString("topics");


    String JDBC_DRIVER = Config.getString("jdbc.driver");
    Integer JDBC_DATASOURCE_SIZE = Config.getInt("jdbc.datasource.size");
    String JDBC_URL = Config.getString("jdbc.url");
    String JDBC_USER = Config.getString("jdbc.user");
    String JDBC_PASSWORD = Config.getString("jdbc.password");


    String CAR_FLOW_OUT_PUT_PATH = Config.getString("car_flow_out_put_path");
    String CAR_FLOW_OUT_PUT_PATH_TMP = Config.getString("car_flow_out_put_path_tmp");

    String CAR_FLOW_CHECKPOINT = Config.getString("car_flow_checkpoint");

}
