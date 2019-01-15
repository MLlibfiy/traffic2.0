
nohup flume-ng agent -c conf -f ../conf/FlumeToKafka.properties -name agent -Dflume.root.logger=INFO,console >> ../logs/flume_to_kafka.log 2>&1 &