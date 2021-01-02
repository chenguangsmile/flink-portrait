package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.EmailInfo;
import com.zhangbao.portrait.map.EmailMap;
import com.zhangbao.portrait.reduce.EmailReduce;
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
public class EmailTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet<EmailInfo> mapResult = text.map(new EmailMap());
        DataSet<EmailInfo> reduceResult = mapResult.groupBy("groupField").reduce(new EmailReduce());
        try {
            List<EmailInfo> result = reduceResult.collect();
            for (EmailInfo emailInfo : result) {
                String emailType = emailInfo.getEmailType();
                Long count = emailInfo.getCount();
                Document doc = MongoUtils.findoneby("email-type-statics", "flink-portrait", emailType);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",emailType);
                    doc.put("count",count);
                }else {
                    Long oldCount = doc.getLong("count");
                    doc.put("count",oldCount + count);
                }
                MongoUtils.saveorupdatemongo("email-type-statics", "flink-portrait",doc);
            }
            env.execute("email type task");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
