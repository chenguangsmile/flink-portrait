package com.zhangbao.log;

import lombok.Data;

import java.io.Serializable;

/**
 * 用户购物车行为
 * @author zhangbao
 * @date 2020/11/21 22:44
 **/
@Data
public class BuyCarProductLog implements Serializable {
    private int productId;//商品id
    private int productType;//商品类型
    private String operateTime;//操作时间
    private int operateType;//操作类型，0：加入，1：删除
    private int userId;
    private int userType;//终端类型，0：pc端，1：移动端，2：小程序
    private String ip;//用户ip
    private String brand;//品牌
}
