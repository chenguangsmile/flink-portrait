package com.zhangbao.portrait.logic;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author zhangbao
 * @date 2020/12/7 22:48
 **/
public class LogicReduce implements GroupReduceFunction<LogicInfo, ArrayList<Double>> {
    @Override
    public void reduce(Iterable<LogicInfo> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        CreateDataSet createDataSet = new CreateDataSet();
        Iterator<LogicInfo> iterator = iterable.iterator();
        while (iterator.hasNext()){
            LogicInfo next = iterator.next();

            ArrayList<String> data = Lists.newArrayList();
            data.add(next.getVariable1());
            data.add(next.getVariable2());
            data.add(next.getVariable3());
            createDataSet.data.add(data);

            createDataSet.labels.add(next.getLabel());
        }
        ArrayList<Double> weigth = Logistic.gradAscent1(createDataSet, createDataSet.labels, 150);
        collector.collect(weigth);
    }
}
