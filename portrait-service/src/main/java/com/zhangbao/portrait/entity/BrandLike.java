package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/11/23 23:03
 **/
@Data
public class BrandLike {
    private String brand;
    private long count;
    private String groupField;
}
