package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.ConsumptionLevel;
import com.zhangbao.portrait.entity.YearBase;
import com.zhangbao.portrait.map.ConsumptionLevelMap;
import com.zhangbao.portrait.map.YearBaseMap;
import com.zhangbao.portrait.reduce.ConsumptionLevelFinalReduce;
import com.zhangbao.portrait.reduce.ConsumptionLevelReduce;
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
public class ConsumptionLevelTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<ConsumptionLevel> mapResult = text.map(new ConsumptionLevelMap());
        DataSet<ConsumptionLevel> reduceResult = mapResult.groupBy("groupField").reduceGroup(new ConsumptionLevelReduce());
        DataSet<ConsumptionLevel> finalResult = reduceResult.groupBy("groupField").reduce(new ConsumptionLevelFinalReduce());

        try {
            List<ConsumptionLevel> result = reduceResult.collect();
            for (ConsumptionLevel consumptionLevel : result) {
                String consumptionType = consumptionLevel.getConsumptionType();
                Long count = consumptionLevel.getCount();
                Document doc = MongoUtils.findoneby("consumption-type-statics", "flink-portrait", consumptionType);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",count);
                    doc.put("count",count);
                }else {
                    Long oldCount = doc.getLong("count");
                    doc.put("count",oldCount + count);
                }
                MongoUtils.saveorupdatemongo("consumption-type-statics", "flink-portrait",doc);
            }
            env.execute("consumption type task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
