package com.zhangbao.portrait.map;

import cn.hutool.core.date.DateTime;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.zhangbao.portrait.entity.UserGroupInfo;
import com.zhangbao.portrait.utils.DateUtils;
import com.zhangbao.utils.ReadProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import scala.Int;

import java.util.*;

/**
 * 计算每一个用户的各项指标
 * 平均消费金额，消费最大金额，消费频次，
 * 消费类目，1电子（手机，电脑，电视等），2生活家居（衣服，生活用品，洗漱用品等），3生鲜（米，油，盐等）
 * 消费时间点，上午（7-12），下去（12-7），晚上（7-12），凌晨（0-7）
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class UserGroupMapByReduce implements MapFunction<UserGroupInfo, UserGroupInfo> {
    private Config config = null;

    public UserGroupMapByReduce() {
        config = ReadProperties.Load("productType.properties");
    }

    @Override
    public UserGroupInfo map(UserGroupInfo userGroupInfo) throws Exception {

        List<UserGroupInfo> list = userGroupInfo.getList();

        double sumAmount = 0D;//总金额
        double maxAmount = Double.MIN_VALUE;//最大金额
        Map<Integer,Integer> frequencyMap = Maps.newHashMap();//消费频率，隔相同天数下多少单
        UserGroupInfo infoBefore = null;
        Map<String,Long> productTypeMap = Maps.newHashMap();//消费类目map
        productTypeMap.put("1",0L);
        productTypeMap.put("2",0L);
        productTypeMap.put("3",0L);
        Map<Integer,Long> timeMap = Maps.newHashMap();//时间点map
        timeMap.put(1,0L);
        timeMap.put(2,0L);
        timeMap.put(3,0L);
        timeMap.put(4,0L);
        for (UserGroupInfo groupInfo : list) {
            Double totalAmount = Double.valueOf(groupInfo.getTotalAmount());
            sumAmount += totalAmount;
            if(totalAmount>maxAmount){
                maxAmount = totalAmount;
            }
            if(infoBefore == null){
                infoBefore = groupInfo;
                continue;
            }
            //计算购买频率，几天内购买几次
            int days = DateUtils.getDaysBetweenDate(infoBefore.getCreateTime(), groupInfo.getCreateTime(), "yyyyMMdd HHmmss");
            int total = frequencyMap.get(days)==null?0:frequencyMap.get(days);
            frequencyMap.put(days,total+1);

            //消费类目
            String productType = groupInfo.getProductType();
            String string = config.getString(productType);
            Long count = productTypeMap.get(string)==null?0L:productTypeMap.get(string);
            productTypeMap.put(string,count+1);

            //时间点
            String createTime = groupInfo.getCreateTime();
            DateTime dateTime = DateUtils.parse(createTime, "yyyyMMdd HHmmss");
            int hour = dateTime.hour(true);//24小时制
            Integer timeType = 0;
            if(hour>=7 && hour <12){
                timeType = 1;
            }else if(hour>=12 && hour <19){
                timeType = 2;
            }else if(hour>=19 && hour <=24){
                timeType = 3;
            }else if(hour>=0 && hour <7){
                timeType = 4;
            }
            Long timePre = timeMap.get(timeType)==null?0L:timeMap.get(timeType);
            timeMap.put(timeType,timePre+1);

        }
        double avrAmount = sumAmount/list.size();//平均消费金额
        Set<Map.Entry<Integer, Integer>> entries = frequencyMap.entrySet();
        Integer totalDays = 0;
        for (Map.Entry<Integer, Integer> entry : entries) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();
            totalDays += key*value;
        }
        int avrDays = totalDays/list.size();//消费频次

        Random random = new Random();
        UserGroupInfo finalInfo = new UserGroupInfo();
        finalInfo.setUserId(userGroupInfo.getUserId());
        finalInfo.setAvrAmount(avrAmount);
        finalInfo.setMaxAmount(maxAmount);
        finalInfo.setDays(avrDays);
        finalInfo.setBuyType1(productTypeMap.get("1"));
        finalInfo.setBuyType2(productTypeMap.get("2"));
        finalInfo.setBuyType3(productTypeMap.get("3"));
        finalInfo.setBuyTime1(timeMap.get(1));
        finalInfo.setBuyTime2(timeMap.get(2));
        finalInfo.setBuyTime3(timeMap.get(3));
        finalInfo.setBuyTime4(timeMap.get(4));
        finalInfo.setGroupField("userGroupKmeans="+random.nextInt(100));

        return finalInfo;
    }

}
