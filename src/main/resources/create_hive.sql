CREATE EXTERNAL TABLE IF NOT EXISTS car_flow(
day  string  comment '日期',
monitor_id  string  comment '卡口编号',
camera_id  string  comment '摄像头编号',
car  string  comment '车牌号',
action_time  string  comment '发生时间',
speed  int  comment '速度',
road_id  string  comment '道路编号',
area_id   string  comment '区域编号'
)PARTITIONED BY (time string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location '/data/traffic/carflow/';

#增加分区
alter table car_flow add if not exists partition(time='201901171635') ;

#查看分区
show partitions car_flow;

同步hdfs分区信息（增加所有分区）
msck repair table car_flow;


一分钟执行一次（一分钟一个分区）

crontab
1、不好管理
2、不好监控
3、不好确定执行完成时间


azkaban --调度框架


每秒执行一次合并操作
*/1 * * * * nohup  bash /root/traffic-2.0/bin/merge_car_flow_tmp_data.sh >> /root/traffic-2.0/logs/merge_car_flow_tmp_data.log 2>&1 &