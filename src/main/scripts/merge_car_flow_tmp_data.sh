source /etc/profile

spark-submit --master local --class com.shujia.rtmroad.MergeCarFlowTmpData /root/traffic-2.0/lib/traffic-2.0.jar

#增加hive分区
/root/apache-hive-1.2.1-bin/bin/hive -e "msck repair table car_flow;"