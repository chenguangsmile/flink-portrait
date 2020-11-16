package com.zhangbao.portrait.utils;

import cn.hutool.core.date.DateUtil;

import java.time.LocalDateTime;

/**
 * @author zhangbao
 * @date 2020/11/15 21:46
 **/
public class DateUtils extends DateUtil {

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

    public static void main(String[] args) {
        System.out.println(getYearBaseByAge("32"));
    }

}
