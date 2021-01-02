package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/11/19 0:01
 **/
@Data
public class UserTypeInfo {
    private String userType;//终端类型
    private Long count;//数量
    private String groupField;//分组字段
}
