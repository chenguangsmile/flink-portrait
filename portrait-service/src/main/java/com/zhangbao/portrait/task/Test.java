package com.zhangbao.portrait.task;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author zhangbao
 * @date 2020/11/15 0:25
 **/
public class Test {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        //set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        //get input data
        DataSource<String> text = env.readTextFile(params.get("input"));
        DataSet map = text.flatMap(null);
        DataSet reduce = map.groupBy("groupbyfield").reduce(null);
        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
