package com.zhangbao.portrait.task;

import com.google.common.collect.Maps;
import com.zhangbao.portrait.entity.BaiJiaInfo;
import com.zhangbao.portrait.map.BaiJiaMap;
import com.zhangbao.portrait.reduce.BaiJiaReduce;
import com.zhangbao.portrait.utils.DateUtils;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * @author zhangbao
 * @date 2020/11/15 21:19
 **/
public class BaiJiaTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<BaiJiaInfo> mapResult = text.map(new BaiJiaMap());
        DataSet<BaiJiaInfo> reduceResult = mapResult.groupBy("groupField").reduce(new BaiJiaReduce());
        try {
            List<BaiJiaInfo> result = reduceResult.collect();
            for (BaiJiaInfo baiJiaInfo : result) {
                String userId = baiJiaInfo.getUserId();
                List<BaiJiaInfo> list = baiJiaInfo.getList();
                Collections.sort(list,new BaiJiaInfo());

                BaiJiaInfo before = null;
                Map<Integer,Integer> frequencyMap = Maps.newHashMap();//频率，隔相同天数下多少单
                //最大金额
                double maxAmount = 0;
                double sum = 0;
                for (BaiJiaInfo jiaInfo : list) {
                    if(before == null){
                        before = jiaInfo;
                        continue;
                    }
                    //计算购买频率，几天内购买几次
                    int days = DateUtils.getDaysBetweenDate(before.getCreateTime(), jiaInfo.getCreateTime(), "yyyyMMdd HHmmss");
                    int total = frequencyMap.get(days)==null?0:frequencyMap.get(days);
                    frequencyMap.put(days,total + 1);
                    //计算最大值
                    Double totalAmount = Double.valueOf(jiaInfo.getTotalAmount());
                    if(totalAmount > maxAmount){
                        maxAmount = totalAmount;
                    }
                    //计算平均值
                    sum += totalAmount;
                    before = jiaInfo;//最后重新赋值
                }
                //平均值
                double avgAmount = sum/list.size();
                //计算下单频次，每单隔几天
                Set<Map.Entry<Integer, Integer>> entries = frequencyMap.entrySet();
                double totalDays = 0;
                for (Map.Entry<Integer, Integer> entry : entries) {
                    totalDays += entry.getKey() * entry.getValue();
                }
                //下单频率
                double avgDay = totalDays/list.size();

                //每一个用户败家指数：支付金额平均值*0.3，最大支付金额*0.3，下单频率*0.4
                //支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100  ）
                // 最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
                // 下单平率40分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avgAmountScore = 0;
                if(avgAmount>=0 && avgAmount < 20){
                    avgAmountScore = 5;
                }else if (avgAmount>=20 && avgAmount < 60){
                    avgAmountScore = 10;
                }else if (avgAmount>=60 && avgAmount < 100){
                    avgAmountScore = 20;
                }else if (avgAmount>=100 && avgAmount < 150){
                    avgAmountScore = 30;
                }else if (avgAmount>=150 && avgAmount < 200){
                    avgAmountScore = 40;
                }else if (avgAmount>=200 && avgAmount < 250){
                    avgAmountScore = 60;
                }else if (avgAmount>=250 && avgAmount < 350){
                    avgAmountScore = 70;
                }else if (avgAmount>=350 && avgAmount < 450){
                    avgAmountScore = 80;
                }else if (avgAmount>=450 && avgAmount < 600){
                    avgAmountScore = 90;
                }else if (avgAmount>=600){
                    avgAmountScore = 100;
                }
                //最大支付金额
                int maxAmountScore = 0;
                if(maxAmount>=0 && maxAmount < 20){
                    maxAmountScore = 5;
                }else if (maxAmount>=20 && maxAmount < 60){
                    maxAmountScore = 10;
                }else if (maxAmount>=60 && maxAmount < 200){
                    maxAmountScore = 30;
                }else if (maxAmount>=200 &&maxAmount < 500){
                    maxAmountScore = 60;
                }else if (maxAmount>=500 && maxAmount < 700){
                    maxAmountScore = 80;
                }else if (maxAmount>=700){
                    maxAmountScore = 100;
                }

                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avgDaysScore = 0;
                if(avgDay>=0 && avgDay < 5){
                    avgDaysScore = 100;
                }else if (avgAmount>=5 && avgAmount < 10){
                    avgDaysScore = 90;
                }else if (avgAmount>=10 && avgAmount < 30){
                    avgDaysScore = 70;
                }else if (avgAmount>=30 && avgAmount < 60){
                    avgDaysScore = 60;
                }else if (avgAmount>=60 && avgAmount < 80){
                    avgDaysScore = 40;
                }else if (avgAmount>=80 && avgAmount < 100){
                    avgDaysScore = 20;
                }else if (avgAmount>=100){
                    avgDaysScore = 10;
                }
                double totalScore = (avgAmountScore/100)*30+(maxAmountScore/100)*30+(avgDaysScore/100)*40;

                String tableName = "userFlagInfo";
                String row = userId;
                String columnFamily = "baseInfo";
                String column = "baijiaScore";//用户败家指数
                //将用户败家指数信息存入HBase中
                HBaseUtils.putData(tableName,row,columnFamily,column,totalScore+"");
            }
            env.execute("baijiaScore analyze task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
