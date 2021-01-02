package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.UserGroupInfo;
import com.zhangbao.portrait.kmeans.*;
import com.zhangbao.portrait.logic.CreateDataSet;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * @author zhangbao
 * @date 2020/12/7 22:48
 **/
public class UserGroupByKmeansReduce implements GroupReduceFunction<UserGroupInfo, ArrayList<Point>> {
    @Override
    public void reduce(Iterable<UserGroupInfo> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        CreateDataSet createDataSet = new CreateDataSet();
        Iterator<UserGroupInfo> iterator = iterable.iterator();
        ArrayList<float[]> dataSet = new ArrayList<float[]>();
        while (iterator.hasNext()){
            UserGroupInfo next = iterator.next();
            float[] f = {Float.valueOf(next.getUserId()),Float.valueOf(next.getAvrAmount()+""),Float.valueOf(next.getMaxAmount()+"")
                ,Float.valueOf(next.getDays()),Float.valueOf(next.getBuyType1()),Float.valueOf(next.getBuyType2()),Float.valueOf(next.getBuyType3())
                ,Float.valueOf(next.getBuyTime1()),Float.valueOf(next.getBuyTime2()),Float.valueOf(next.getBuyTime3()),Float.valueOf(next.getBuyTime4())};
            dataSet.add(f);
        }

        KMeansRunByUserGroup run = new KMeansRunByUserGroup(6,dataSet);
        Set<Cluster> clusterSet = run.run();
        ArrayList<Point> center = new ArrayList<>();
        for (Cluster cluster : clusterSet) {
            center.add(cluster.getCenter());
        }
        collector.collect(center);
    }
}
