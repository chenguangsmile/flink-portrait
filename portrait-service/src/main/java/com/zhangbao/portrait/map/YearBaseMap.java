package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.YearBase;
import com.zhangbao.portrait.utils.DateUtils;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class YearBaseMap implements MapFunction<String, YearBase> {
    @Override
    public YearBase map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] split = s.split(",");
        String userId = split[0];
        String userName = split[1];
        String password = split[2];
        String sex = split[3];
        String telphone = split[4];
        String email = split[5];
        String age = split[6];
        String registerTime = split[7];
        String userType = split[8];//用户类型，0：pc，1：移动，2：小程序
        String tableName = "userFlagInfo";
        String row = userId;
        String columnFamily = "baseInfo";
        String column = "yearBase";

        String yearType = DateUtils.getYearBaseByAge(age);//年代
        //将用户年代信息存入HBase中
        HBaseUtils.putData(tableName,row,columnFamily,column,yearType);
        YearBase yearBase = new YearBase();
        yearBase.setCount(1L);
        yearBase.setYearType(yearType);
        yearBase.setGroupField("yearBase==" + yearType);
        return yearBase;
    }
}
