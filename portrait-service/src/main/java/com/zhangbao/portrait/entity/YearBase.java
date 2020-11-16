package com.zhangbao.portrait.entity;

import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/11/15 21:21
 **/
@Data
public class YearBase {
    private String yearType;//年代类型
    private Long count;//总数
    private String groupField;//分组字段
}
