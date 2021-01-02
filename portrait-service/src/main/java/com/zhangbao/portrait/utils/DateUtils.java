package com.zhangbao.portrait.utils;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;

import java.time.LocalDateTime;

/**
 * @author zhangbao
 * @date 2020/11/15 21:46
 **/
public class DateUtils extends DateUtil {

    /**
     * 获取年龄对应的年代
     * @param age
     * @return
     */
    public static String getYearBaseByAge(String age){
        String yearBaseType = "未知";
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime localDateTime = now.minusYears(Long.valueOf(age));
        int year = localDateTime.getYear();
        if(year >= 1940 && year < 1950){
            yearBaseType = "40后";
        } else if(year >= 1950 && year < 1960){
            yearBaseType = "50后";
        } else if(year >= 1960 && year < 1970){
            yearBaseType = "60后";
        } else if(year >= 1970 && year < 1980){
            yearBaseType = "70后";
        } else if(year >= 1980 && year < 1990){
            yearBaseType = "80后";
        } else if(year >= 1990 && year < 2000){
            yearBaseType = "90后";
        } else if(year >= 2000 && year < 2010){
            yearBaseType = "00后";
        } else if(year >= 2010){
            yearBaseType = "10后";
        }
        return yearBaseType;
    }

    /**
     * 获取两个时间差多天
     * @param startDateStr
     * @param endDateStr
     * @param patterFormat
     * @return
     */
    public static int getDaysBetweenDate(String startDateStr, String endDateStr, String patterFormat){
        DateTime startDate = DateUtil.parse(startDateStr, patterFormat);
        DateTime endDate = DateUtil.parse(endDateStr, patterFormat);
        long l = DateUtil.betweenDay(startDate, endDate, true);
        Integer value = Integer.valueOf(Long.valueOf(l).toString());
        return value;

    }

    public static void main(String[] args) {
        String startStr = "2020-12-01";
        String endStr = "2020-12-04";
        long daysBetweenDate = getDaysBetweenDate(startStr, endStr, "yyyy-MM-dd");
        System.out.println(daysBetweenDate);
    }

}
