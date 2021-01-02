package com.zhangbao.portrait.task;

import com.zhangbao.portrait.entity.BrandLike;
import com.zhangbao.portrait.kafka.CustomWatermarkExtractor;
import com.zhangbao.portrait.kafka.KafkaEvent;
import com.zhangbao.portrait.kafka.KafkaEventSchema;
import com.zhangbao.portrait.map.BrandLikeMap;
import com.zhangbao.portrait.reduce.BrandLikeReduce;
import com.zhangbao.portrait.reduce.BrandLikeSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * @author zhangbao
 * @date 2020/11/22 17:27
 **/
public class BrandLikeTask {
    public static void main(String[] args) throws Exception {
            // parse input arguments
            args = new String[]{"--input-topic","scanProductLog"," --bootstrap.servers "," hadoop101:9092,hadoop102:9092,hadoop103:9092 ",
                    "--zookeeper.connect "," hadoop101:2181,hadoop102:2181,hadoop103:2181 ","--group.id "," zhangbao "};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<KafkaEvent> input;
        input = env.addSource(new FlinkKafkaConsumer010<>
                        (parameterTool.getRequired("input-topic"),
                                new KafkaEventSchema(),
                                parameterTool.getProperties()
                        ).assignTimestampsAndWatermarks(new CustomWatermarkExtractor()));
        DataStream<BrandLike> flatMap = input.flatMap(new BrandLikeMap());

        DataStream<BrandLike> reduce = flatMap.keyBy("groupField").timeWindowAll(Time.seconds(2)).reduce(new BrandLikeReduce());

        reduce.addSink(new BrandLikeSink());

        env.execute("brand like analyse");
    }
}
