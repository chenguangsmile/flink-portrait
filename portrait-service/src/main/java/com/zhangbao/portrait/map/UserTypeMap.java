package com.zhangbao.portrait.map;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.zhangbao.log.ScanProductLog;
import com.zhangbao.portrait.entity.BrandLike;
import com.zhangbao.portrait.entity.UserTypeInfo;
import com.zhangbao.portrait.kafka.KafkaEvent;
import com.zhangbao.portrait.utils.HBaseUtils;
import com.zhangbao.portrait.utils.MapUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @author zhangbao
 * @date 2020/11/23 23:03
 **/
public class UserTypeMap implements FlatMapFunction<KafkaEvent, UserTypeInfo> {
    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<UserTypeInfo> collector) throws Exception {
        String word = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(word, ScanProductLog.class);
        int userId = scanProductLog.getUserId();
        int userType = scanProductLog.getUserType();//0：pc，1：移动，2：小程序
        String userTypeName = userType==0?"pc端":userType==1?"移动端":"小程序端";

        String tableName = "userFlagInfo";
        String row = userId+"";
        String columnFamily = "userBehavior";
        String column = "userTypeList";//用户终端浏览列表
        String dataMap = HBaseUtils.getdata(tableName, row, columnFamily, column);
        Map<String,Long> map = Maps.newHashMap();
        if(StrUtil.isNotBlank(dataMap)){
            map = JSONObject.parseObject(dataMap, Map.class);
        }
        //获取之前终端偏好
        String preMaxUserType = MapUtils.getMaxBrandLike(map);
        Long oldCount = map.get(userTypeName)==null?0L:map.get(userTypeName);
        map.put(userTypeName,oldCount + 1);
        HBaseUtils.putData(tableName,row,columnFamily,column,JSONObject.toJSONString(map));

        //获取最大终端偏好
        String maxUserType = MapUtils.getMaxBrandLike(map);
        if(StrUtil.isNotBlank(preMaxUserType) && !preMaxUserType.equals(maxUserType)){
            UserTypeInfo userTypeInfo = new UserTypeInfo();
            userTypeInfo.setUserType(preMaxUserType);
            userTypeInfo.setCount(-1L);
            userTypeInfo.setGroupField("=groupField="+preMaxUserType);
            collector.collect(userTypeInfo);
        }
        UserTypeInfo userTypeInfo = new UserTypeInfo();
        userTypeInfo.setUserType(maxUserType);
        userTypeInfo.setCount(1L);
        userTypeInfo.setGroupField("=groupField="+maxUserType);
        collector.collect(userTypeInfo);

        column = "userType";//最大终端偏好
        HBaseUtils.putData(tableName, row, columnFamily, column,maxUserType);
    }
}
