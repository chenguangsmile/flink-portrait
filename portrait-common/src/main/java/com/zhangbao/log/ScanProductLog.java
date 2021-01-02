package com.zhangbao.log;

import lombok.Data;

import java.io.Serializable;

/**
 * 用户浏览商品行为
 * @author zhangbao
 * @date 2020/11/21 22:11
 **/
@Data
public class ScanProductLog implements Serializable {

    private int productId;//商品id
    private int productType;//商品分类
    private String scanTime;//浏览时间
    private String stayTime;//停留时间
    private int userId;//用户id
    private int userType;//终端类型，0：pc，1：移动，2：小程序
    private String ip;//用户ip
    private String brand;//品牌

}
