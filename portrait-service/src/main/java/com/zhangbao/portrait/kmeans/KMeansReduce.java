package com.zhangbao.portrait.kmeans;

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
public class KMeansReduce implements GroupReduceFunction<KMeans, ArrayList<Point>> {
    @Override
    public void reduce(Iterable<KMeans> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        CreateDataSet createDataSet = new CreateDataSet();
        Iterator<KMeans> iterator = iterable.iterator();
        ArrayList<float[]> dataSet = new ArrayList<float[]>();
        while (iterator.hasNext()){
            KMeans next = iterator.next();
            float[] f = {Float.valueOf(next.getVariable1()),Float.valueOf(next.getVariable2()),Float.valueOf(next.getVariable3())};
            dataSet.add(f);
        }

        KMeansRun run = new KMeansRun(6,dataSet);
        Set<Cluster> clusterSet = run.run();
        ArrayList<Point> center = new ArrayList<>();
        for (Cluster cluster : clusterSet) {
            center.add(cluster.getCenter());
        }
        collector.collect(center);
    }
}
