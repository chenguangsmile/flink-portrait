package com.zhangbao.portrait.entity;

import lombok.Data;

import java.util.List;

/**
 * @author zhangbao
 * @date 2020/12/26 21:10
 **/
@Data
public class ChaoManWomenInfo {
    private String userId;
    private Long count;//数量
    private String chaoType;//潮男1，潮女2
    private String groupField;

    private List<ChaoManWomenInfo> list;

}
