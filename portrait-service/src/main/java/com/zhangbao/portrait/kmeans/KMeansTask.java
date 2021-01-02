package com.zhangbao.portrait.kmeans;

import com.google.common.collect.Lists;
import com.zhangbao.portrait.logic.LogicInfo;
import com.zhangbao.portrait.logic.LogicMap;
import com.zhangbao.portrait.logic.LogicReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
public class KMeansTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<KMeans> mapResult = text.map(new KMeansMap());
        DataSet<ArrayList<Point>> reduceResult = mapResult.groupBy("groupField").reduceGroup(new KMeansReduce());
        try {
            List<ArrayList<Point>> result = reduceResult.collect();
            ArrayList<float[]> dataSet = new ArrayList<>();
            for (ArrayList<Point> array : result) {
                for (Point point : array) {
                    dataSet.add(point.getLocalArray());
                }
            }
            //找出中心点
            KMeansRun run = new KMeansRun(6,dataSet);
            Set<Cluster> clusterSet = run.run();
            ArrayList<Point> finalCenter = new ArrayList<>();
            int count = 100;
            for (Cluster cluster : clusterSet) {
                Point point = cluster.getCenter();
                point.setId(count++);
                finalCenter.add(point);
            }
            //给点打标签，使其属于哪个簇(类)
            DataSet<Point> finalMap = text.map(new KMeansFinalMap(finalCenter));
            //打完标签并存入hdfs中
            finalMap.writeAsText(params.get("out"));

            env.execute("kmeans task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
