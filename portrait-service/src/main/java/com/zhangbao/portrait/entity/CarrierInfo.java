package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/11/18 23:00
 **/
@Data
public class CarrierInfo {
    private String carrier;//运营商
    private Long count;//数量
    private String groupField;//分组
}
