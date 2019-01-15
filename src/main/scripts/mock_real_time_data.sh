mkdir /tmp/flume
nohup hadoop jar ../lib/traffic-2.0.jar com.shujia.util.MockRealTimeData >> /tmp/flume/data.log 2>&1 &