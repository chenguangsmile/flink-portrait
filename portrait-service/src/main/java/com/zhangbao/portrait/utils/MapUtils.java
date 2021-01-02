package com.zhangbao.portrait.utils;

import cn.hutool.core.map.MapUtil;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author zhangbao
 * @date 2020/11/23 23:37
 **/
public class MapUtils extends MapUtil {

    public static String getMaxBrandLike(Map<String,Long> map){
        if(map.isEmpty()){
            return null;
        }
        TreeMap<Long,String> treeMap = new TreeMap<>(new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o2.compareTo(o1);
            }
        });
        Set<Map.Entry<String, Long>> entrySet = map.entrySet();
        for (Map.Entry<String, Long> entry : entrySet) {
            String key = entry.getKey();
            Long value = entry.getValue();
            treeMap.put(value,key);
        }
        Long aLong = treeMap.firstKey();
        return treeMap.get(aLong);
    }

}
