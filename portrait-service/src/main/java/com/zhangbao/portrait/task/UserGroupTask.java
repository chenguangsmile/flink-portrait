package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.UserGroupInfo;
import com.zhangbao.portrait.kmeans.*;
import com.zhangbao.portrait.map.*;
import com.zhangbao.portrait.reduce.UserGroupReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author zhangbao
 * @date 2020/11/15 21:19
 **/
public class UserGroupTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<UserGroupInfo> mapResult = text.map(new UserGroupMap());
        DataSet<UserGroupInfo> reduceResult = mapResult.groupBy("groupField").reduce(new UserGroupReduce());
        DataSet<UserGroupInfo> finalResult = reduceResult.map(new UserGroupMapByReduce());
        DataSet<ArrayList<Point>> kemeansResult = finalResult.groupBy("groupField").reduceGroup(new UserGroupByKmeansReduce());
        try {
            List<ArrayList<Point>> result = kemeansResult.collect();
            ArrayList<float[]> dataSet = new ArrayList<>();
            for (ArrayList<Point> array : result) {
                for (Point point : array) {
                    dataSet.add(point.getLocalArray());
                }
            }
            //找出中心点
            KMeansRunByUserGroup run = new KMeansRunByUserGroup(6,dataSet);
            Set<Cluster> clusterSet = run.run();
            ArrayList<Point> finalCenter = new ArrayList<>();
            int count = 100;
            for (Cluster cluster : clusterSet) {
                Point point = cluster.getCenter();
                point.setId(count++);
                finalCenter.add(point);
            }
            //给点打标签，使其属于哪个簇(类)，并把用户分群信息存入hbase中
            DataSet<Point> finalMap = reduceResult.map(new KMeansFinalUserGroupMap(finalCenter));
            env.execute("kmeans by userGroup task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
