package com.zhangbao.portrait.reduce;

import com.google.common.collect.Lists;
import com.zhangbao.portrait.entity.SexPreInfo;
import com.zhangbao.portrait.logic.CreateDataSet;
import com.zhangbao.portrait.logic.LogicInfo;
import com.zhangbao.portrait.logic.Logistic;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author zhangbao
 * @date 2020/12/7 22:48
 **/
public class SexPreReduce implements GroupReduceFunction<SexPreInfo, ArrayList<Double>> {
    @Override
    public void reduce(Iterable<SexPreInfo> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        CreateDataSet createDataSet = new CreateDataSet();
        Iterator<SexPreInfo> iterator = iterable.iterator();
        while (iterator.hasNext()){
            SexPreInfo next = iterator.next();
            ArrayList<String> data = Lists.newArrayList();
            int userId = next.getUserId();
            long orderNum = next.getOrderNum();
            long orderFre = next.getOrderFre();
            int manClothes = next.getManClothes();
            int womenClothes = next.getWomenClothes();
            int childClothes = next.getChildClothes();
            int oldManClothes = next.getOldManClothes();
            double avrAmount = next.getAvrAmount();
            int productItems = next.getProductItems();
            int label = next.getLabel();

            data.add(orderNum+"");
            data.add(orderFre+"");
            data.add(manClothes+"");
            data.add(womenClothes+"");
            data.add(childClothes+"");
            data.add(oldManClothes+"");
            data.add(avrAmount+"");
            data.add(productItems+"");

            createDataSet.data.add(data);

            createDataSet.labels.add(label+"");
        }
        ArrayList<Double> weigth = Logistic.gradAscent1(createDataSet, createDataSet.labels, 500);
        collector.collect(weigth);
    }
}
