/*********************************************************************
 * 
 * CHINA TELECOM CORPORATION CONFIDENTIAL
 * ______________________________________________________________
 * 
 *  [2015] - [2020] China Telecom Corporation Limited, 
 *  All Rights Reserved.
 * 
 * NOTICE:  All information contained herein is, and remains
 * the property of China Telecom Corporation and its suppliers,
 * if any. The intellectual and technical concepts contained 
 * herein are proprietary to China Telecom Corporation and its 
 * suppliers and may be covered by China and Foreign Patents,
 * patents in process, and are protected by trade secret  or 
 * copyright law. Dissemination of this information or 
 * reproduction of this material is strictly forbidden unless prior 
 * written permission is obtained from China Telecom Corporation.
 **********************************************************************/
package com.shujia.common.boot;


import com.shujia.common.Config;
import org.apache.hadoop.conf.Configuration;


public class HadoopUtil {
	private static Configuration conf;
	public static Configuration getHadoopConfig(){
		if(conf==null){
			synchronized (HadoopUtil.class) { 
				if(conf==null){
					Configuration configuration= new Configuration();
					Config.writeConfig2Hadoop(configuration);
					conf = configuration;
				}
			}
		}
		return conf;
		
	}
}
