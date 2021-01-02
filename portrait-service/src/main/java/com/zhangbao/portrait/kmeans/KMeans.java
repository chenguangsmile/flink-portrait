package com.zhangbao.portrait.kmeans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zhangbao
 * @date 2020/12/7 22:31
 **/
@Data
@AllArgsConstructor
public class KMeans {
    private String variable1;
    private String variable2;
    private String variable3;
    private String label;
    private String groupByField;
}
