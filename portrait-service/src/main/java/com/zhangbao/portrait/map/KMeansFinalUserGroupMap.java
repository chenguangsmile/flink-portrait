package com.zhangbao.portrait.map;

import com.alibaba.fastjson.JSONObject;
import com.zhangbao.portrait.entity.UserGroupInfo;
import com.zhangbao.portrait.kmeans.DistanceCompute;
import com.zhangbao.portrait.kmeans.Point;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
public class KMeansFinalUserGroupMap implements MapFunction<UserGroupInfo, Point> {
    List<Point> centers = new ArrayList<>();
    private DistanceCompute disC = new DistanceCompute();

    public KMeansFinalUserGroupMap(List<Point> centers) {
        this.centers = centers;
    }

    @Override
    public Point map(UserGroupInfo userGroupInfo) throws Exception {


        float[] f = {Float.valueOf(userGroupInfo.getAvrAmount()+""),Float.valueOf(userGroupInfo.getMaxAmount()+"")
                ,Float.valueOf(userGroupInfo.getDays()),Float.valueOf(userGroupInfo.getBuyType1()),Float.valueOf(userGroupInfo.getBuyType2()),Float.valueOf(userGroupInfo.getBuyType3())
                ,Float.valueOf(userGroupInfo.getBuyTime1()),Float.valueOf(userGroupInfo.getBuyTime2()),Float.valueOf(userGroupInfo.getBuyTime3()),Float.valueOf(userGroupInfo.getBuyTime4())};

        Point self = new Point(Integer.valueOf(userGroupInfo.getUserId()),f);
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

        String tableName = "userFlagInfo";
        String row = userGroupInfo.getUserId()+"";
        String columnFamily = "userGroupInfo";
        String column = "userGroupInfo";//用户分群信息
        HBaseUtils.putData(tableName, row, columnFamily, column, JSONObject.toJSONString(self));

        return self;
    }

}
