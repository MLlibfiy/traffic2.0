/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50538
Source Host           : localhost:3306
Source Database       : traffic

Target Server Type    : MYSQL
Target Server Version : 50538
File Encoding         : 65001

Date: 2019-01-15 16:36:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for control_flow
-- ----------------------------
DROP TABLE IF EXISTS `control_flow`;
CREATE TABLE `control_flow` (
  `date` varchar(255) DEFAULT NULL,
  `monitor_id` varchar(255) DEFAULT NULL,
  `camera_id` varchar(255) DEFAULT NULL,
  `car` varchar(255) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `speed` varchar(255) DEFAULT NULL,
  `road_id` varchar(255) DEFAULT NULL,
  `areaId` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
