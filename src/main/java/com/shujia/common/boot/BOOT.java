
package com.shujia.common.boot;


import com.shujia.common.Config;
import com.shujia.util.ArrayUtil;
import com.shujia.util.ClassUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 启动入口类，反射main方法执行
 * @author dingjingbo
 * 2016年5月9日
 */
public class BOOT {
	private static Logger LOGGER = LoggerFactory.getLogger(BOOT.class);

	public static void main(String[] args) throws Exception {
		String property = System.getProperty("user.dir");
		File f = new File(property);
		if(!"bin".equals(f.getName())){
			throw new RuntimeException("请在bin目录下运行程序");
		}else{
			File[] files = f.getParentFile().listFiles();
			List<String> dirs = new ArrayList<String>();
			if(files!=null){
				for(File file : files){
					if(file.isDirectory()){
						dirs.add(file.getName());
					}
				}
			}
			if(!dirs.contains("bin")||!dirs.contains("conf")||!dirs.contains("logs")||!dirs.contains("lib")){
				throw new RuntimeException("请使用约束的包结构");
			}
		}
		Config.loadCustomConfig(HadoopUtil.getHadoopConfig(),Config.getDefaultCustomConfigPath());
		Config.dump();
		Class<?> clazz = Class.forName(args[0]);
		Class<?>[] interfaces = clazz.getInterfaces();
		if(interfaces!=null){
			for(Class<?> clazzz : interfaces){//如果发现类实现的接口里面有tool，则添加--libjars 参数
				if(clazzz==LibJars.class){
					List<String> jarFiles = new ArrayList<String>();
					File jarFile = ClassUtil.getJarFile(clazz); //在hadoop环境下不行 hadoop会先把jar包考到hadoop的环境变量下
					String libJars = ClassUtil.getLibJars(clazz);
					LOGGER.info("jarFile:"+jarFile.getAbsolutePath());
					LOGGER.info("user.dir:"+System.getProperty("user.dir"));
					LOGGER.info("libJars:"+libJars);
					File[] listFiles = new File(new File(System.getProperty("user.dir")).getParentFile(),"lib").listFiles();
					for(File file : listFiles){
						if(file.getName().endsWith(".jar")){//!file.getName().equals(jarFile.getName())&&
							jarFiles.add(file.getAbsolutePath());
						}
					}
					if(jarFiles.size()>0){
						String[] newArgs = new String[2];
						newArgs[0] = "-libjars";
						newArgs[1] = ArrayUtil.join(jarFiles, ",");
						LOGGER.info("args[0]:"+newArgs[0]);
						LOGGER.info("args[1]:"+newArgs[1]);
						new GenericOptionsParser(HadoopUtil.getHadoopConfig(), newArgs).getRemainingArgs();
					}
						
				}
			}
		}
		Method method = clazz.getMethod("main", String[].class);
		String[] copyOfRange = Arrays.copyOfRange(args, 1, args.length);
		method.invoke(null, (Object)copyOfRange);
	}
}
