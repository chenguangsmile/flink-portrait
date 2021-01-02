package com.zhangbao.portrait.kmeans;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
public class KMeansFinalMap implements MapFunction<String, Point> {
    List<Point> centers = new ArrayList<>();
    private DistanceCompute disC = new DistanceCompute();

    public KMeansFinalMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        Random random = new Random();
        String[] split = s.split(",");
        String variable1 = split[0];
        String variable2 = split[1];
        String variable3 = split[2];
        String label = split[3];
        float[] f = {Float.valueOf(variable1),Float.valueOf(variable2),Float.valueOf(variable3)};
        Point self = new Point(100,f);
        float min_dis = Integer.MAX_VALUE;
        for (Point point : centers) {
            float tmp_dis = (float) Math.min(disC.getEuclideanDis(self, point), min_dis);
            if (tmp_dis != min_dis) {
                min_dis = tmp_dis;
                self.setClusterId(point.getId());
                self.setDist(min_dis);
                self.setClusterPoint(point);
            }
        }
        return self;
    }

}
