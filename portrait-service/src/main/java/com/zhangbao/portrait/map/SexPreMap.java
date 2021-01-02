package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.SexPreInfo;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Random;

/**
 * @author zhangbao
 * @date 2020/12/9 23:04
 **/
public class SexPreMap implements MapFunction<String, SexPreInfo> {
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
        int label = Integer.valueOf(split[9]);//标签，男，女

        String groupField = "sexPre="+random.nextInt(10);
        SexPreInfo sexPreInfo = new SexPreInfo();
        sexPreInfo.setUserId(userId);
        sexPreInfo.setOrderNum(orderNum);
        sexPreInfo.setOrderFre(orderFre);
        sexPreInfo.setManClothes(manClothes);
        sexPreInfo.setWomenClothes(womenClothes);
        sexPreInfo.setChildClothes(childClothes);
        sexPreInfo.setOldManClothes(oldManClothes);
        sexPreInfo.setAvrAmount(avrAmount);
        sexPreInfo.setProductItems(productItems);
        sexPreInfo.setLabel(label);
        sexPreInfo.setGroupField(groupField);
        return sexPreInfo;
    }
}
