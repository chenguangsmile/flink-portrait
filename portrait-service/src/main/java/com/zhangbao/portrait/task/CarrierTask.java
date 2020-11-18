package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.CarrierInfo;
import com.zhangbao.portrait.map.CarrierMap;
import com.zhangbao.portrait.reduce.CarrierReduce;
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
public class CarrierTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<CarrierInfo> mapResult = text.map(new CarrierMap());
        DataSet<CarrierInfo> reduceResult = mapResult.groupBy("groupField").reduce(new CarrierReduce());
        try {
            List<CarrierInfo> result = reduceResult.collect();
            for (CarrierInfo carrierInfo : result) {
                String carrierType = carrierInfo.getCarrier();
                Long count = carrierInfo.getCount();
                Document doc = MongoUtils.findoneby("carrier-type-statics", "flink-portrait", carrierType);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",carrierType);
                    doc.put("count",count);
                }else {
                    Long oldCount = doc.getLong("count");
                    doc.put("count",oldCount + count);
                }
                MongoUtils.saveorupdatemongo("carrier-type-statics", "flink-portrait",doc);
            }
            env.execute("carrier type task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
