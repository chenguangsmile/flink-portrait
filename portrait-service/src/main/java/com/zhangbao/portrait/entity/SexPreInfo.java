package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/12/9 22:55
 **/
@Data
public class SexPreInfo {

    private int userId;
    private long orderNum;//订单总数
    private long orderFre;//隔多少天下单
    private int manClothes;//浏览男装次数
    private int womenClothes;//浏览女装次数
    private int childClothes;//浏览童装次数
    private int oldManClothes;//浏览老人衣服次数
    private double avrAmount;//订单平均金额
    private int productItems;//每天浏览商品数
    private int label;//标签，0女，1男

    private String groupField;

}
