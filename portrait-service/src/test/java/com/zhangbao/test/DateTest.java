package com.zhangbao.test;

import cn.hutool.core.date.DateTime;
import com.zhangbao.portrait.utils.DateUtils;

/**
 * @author zhangbao
 * @date 2020/12/24 22:15
 **/
public class DateTest {

    public static void main(String[] args) {
        DateTime dateTime = DateUtils.parse("20201223 221523", "yyyyMMdd HHmmss");
        System.out.println(dateTime.hour(true));
        System.out.println(dateTime.hour(false));
    }

}
