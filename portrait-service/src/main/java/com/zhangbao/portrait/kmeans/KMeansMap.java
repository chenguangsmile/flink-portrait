package com.zhangbao.portrait.kmeans;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
public class KMeansMap implements MapFunction<String, KMeans> {
    @Override
    public KMeans map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        Random random = new Random();
        String[] split = s.split(",");
        String variable1 = split[0];
        String variable2 = split[1];
        String variable3 = split[2];
        String label = split[3];
        String groupByField = "=logic="+random.nextInt(10);
        KMeans kMeans = new KMeans(variable1,variable2,variable3,label,groupByField);

        return kMeans;
    }

}
