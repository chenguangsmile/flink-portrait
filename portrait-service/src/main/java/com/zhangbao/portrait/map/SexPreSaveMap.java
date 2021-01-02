package com.zhangbao.portrait.map;

import com.google.common.collect.Lists;
import com.zhangbao.portrait.entity.SexPreInfo;
import com.zhangbao.portrait.logic.Logistic;
import com.zhangbao.portrait.utils.EmailUtils;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author zhangbao
 * @date 2020/12/9 23:04
 **/
public class SexPreSaveMap implements MapFunction<String, SexPreInfo> {
    private ArrayList<Double> weights = null;

    public SexPreSaveMap(ArrayList<Double> weights) {
        this.weights = weights;
    }

    @Override
    public SexPreInfo map(String s) throws Exception {
        Random random = new Random();
        String[] split = s.split("\t");
        int userId = Integer.valueOf(split[0]);
        long orderNum = Long.valueOf(split[1]);//订单总数
        long orderFre = Long.valueOf(split[2]);//隔多少天下单
        int manClothes = Integer.valueOf(split[3]);//浏览男装次数
        int womenClothes = Integer.valueOf(split[4]);//浏览女装次数
        int childClothes = Integer.valueOf(split[5]);//浏览童装次数
        int oldManClothes = Integer.valueOf(split[6]);//浏览老人衣服次数
        double avrAmount = Double.valueOf(split[7]);//订单平均金额
        int productItems = Integer.valueOf(split[8]);//每天浏览商品数
        int label = Integer.valueOf(split[9]);//标签，1男，0女

        ArrayList<String> data = Lists.newArrayList();

        data.add(orderNum+"");
        data.add(orderFre+"");
        data.add(manClothes+"");
        data.add(womenClothes+"");
        data.add(childClothes+"");
        data.add(oldManClothes+"");
        data.add(avrAmount+"");
        data.add(productItems+"");
        String sexFlag = Logistic.classifyVector(data, weights);
        String sexStr = sexFlag=="0"?"女":"男";

        String tableName = "userFlagInfo";
        String row = userId+"";
        String columnFamily = "baseInfo";
        String column = "sex";

        //将用户性别存入HBase中
        HBaseUtils.putData(tableName,row,columnFamily,column,sexStr);
        return null;
    }
}
