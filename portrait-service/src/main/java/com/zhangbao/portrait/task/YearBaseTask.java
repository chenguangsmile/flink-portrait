package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.YearBase;
import com.zhangbao.portrait.map.YearBaseMap;
import com.zhangbao.portrait.reduce.YearBaseReduce;
import com.zhangbao.portrait.utils.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;

/**
 * @author zhangbao
 * @date 2020/11/15 21:19
 **/
public class YearBaseTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<YearBase> mapResult = text.map(new YearBaseMap());
        DataSet<YearBase> reduceResult = mapResult.groupBy("groupField").reduce(new YearBaseReduce());
        try {
            List<YearBase> result = reduceResult.collect();
            for (YearBase yearBase : result) {
                String yearType = yearBase.getYearType();
                Long count = yearBase.getCount();
                Document doc = MongoUtils.findoneby("year-type-statics", "flink-portrait", yearType);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",yearType);
                    doc.put("count",count);
                }else {
                    Long oldCount = doc.getLong("count");
                    doc.put("count",oldCount + count);
                }
                MongoUtils.saveorupdatemongo("year-type-statics", "flink-portrait",doc);
            }
            env.execute("year base task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
