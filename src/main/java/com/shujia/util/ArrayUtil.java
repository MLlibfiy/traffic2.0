
package com.shujia.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * 数组工具类
 * @author dingjingbo
 */
public class ArrayUtil {
	private ArrayUtil(){
		
	};
	/**
	 * 字符串连接
	 * @param strs 字符串集合
	 * @param joinStr 连接字符
	 * @return 连接后的字符串
	 */
	public static String join(Collection<? extends Object> strs,String joinStr){
		if(strs==null){
			return null;
		}
		int i = 0; 
		StringBuilder sb = new StringBuilder();
		for(Object str : strs){
			if(i==0){
				sb.append(str.toString());
			}else{
				sb.append(joinStr);
				sb.append(str.toString());
			}
			i++;
		}
		return sb.toString();
	}
	/**
	 * 字符串连接
	 * @param strs 字符串数组
	 * @param joinStr 连接字符
	 * @return 连接后的字符串
	 */
	public static String join(String[] strs,String joinStr){
		if(strs==null){
			return null;
		}
		int i = 0; 
		StringBuilder sb = new StringBuilder();
		for(String str : strs){
			if(i==0){
				sb.append(str);
			}else{
				sb.append(joinStr);
				sb.append(str);
			}
			i++;
		}
		return sb.toString();
	}

	/**
	 * 将一个字符串以分割后存入集合中
	 * @param str 源字符串
	 * @param sep 切割字符
	 * @param t 封装集合
	 * @param <T> 封装集合泛型
	 * @return 切割后的集合
	 */
	public static <T extends Collection<String>> T toCollection(String str,String sep,Class<T> t){
		try {
			String[] split = str.split(sep);
			List<String> asList = Arrays.asList(split);
			T t1 = t.newInstance();
			t1.addAll(asList);
			return t1;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	/**
	 * 判断一个字符串数组是否包含某个元素
	 * @param strs 字符串数组
	 * @param str 被判断字符串
	 * @return 如果字符串数组为空，返回false，包含返回true，不包含返回false
	 */
	public static boolean contains(String[] strs,String str){
		if(strs==null){
			return false;
		}
		boolean contains=false;
		for(String s : strs){
			if(s.equals(str)){
				contains = true;
			}
		}
		return contains;
			
	}
	/**
	 * 求平均值
	 * @param numbers 数值集合
	 * @return 平均值
	 */
	public static double avg(Collection<? extends Number> numbers){
		double sum = 0;
		for(Number n : numbers){
			sum = sum + n.doubleValue();
		}
		return sum/numbers.size();
	}
	/**
	 * 求最大值
	 * @param numbers 数值集合
	 * @return 最大值
	 */
	public static double max(Collection<? extends Number> numbers){
		double max = -Double.MAX_VALUE;
		for(Number n : numbers){
			if(n.doubleValue()>max){
				max = n.doubleValue();
			}
		}
		return max;
	}
	/**
	 * 求最小值
	 * @param numbers 数值集合
	 * @return 最小值
	 */
	public static double min(Collection<? extends Number> numbers){
		double min = Double.MAX_VALUE;
		for(Number n : numbers){
			if(n.doubleValue()<min){
				min = n.doubleValue();
			}
		}
		return min;
	}
	
}
