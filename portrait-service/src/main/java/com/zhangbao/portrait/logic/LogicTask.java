package com.zhangbao.portrait.logic;

import com.google.common.collect.Lists;
import com.zhangbao.portrait.entity.CarrierInfo;
import com.zhangbao.portrait.map.CarrierMap;
import com.zhangbao.portrait.reduce.CarrierReduce;
import com.zhangbao.portrait.utils.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.*;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
public class LogicTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<LogicInfo> mapResult = text.map(new LogicMap());
        DataSet<ArrayList<Double>> reduceResult = mapResult.groupBy("groupField").reduceGroup(new LogicReduce());
        try {
            List<ArrayList<Double>> result = reduceResult.collect();
            Map<Integer,Double> sumMap = new TreeMap<Integer,Double>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            for (ArrayList<Double> array : result) {
                for (int i = 0; i < array.size(); i++) {
                    Double pre = sumMap.get(i)==null?0d:sumMap.get(i);
                    sumMap.put(i,pre + array.get(i));
                }
            }
            ArrayList<Double> finalWeigth = Lists.newArrayList();
            Set<Map.Entry<Integer, Double>> entries = sumMap.entrySet();
            for (Map.Entry<Integer, Double> entry : entries) {
                Integer key = entry.getKey();
                Double value = entry.getValue();
                Double weigth = value/result.size();
                finalWeigth.add(weigth);
            }

            env.execute("LogicTask task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
