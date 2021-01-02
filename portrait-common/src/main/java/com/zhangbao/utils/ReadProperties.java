package com.zhangbao.utils;

import cn.hutool.core.util.StrUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * 用户关注行为
 * @author zhangbao
 * @date 2020/11/22 12:44
 **/
public class ReadProperties {
    private static Config config = null;

    /**
     * 加载配置文件
     * @param pro 文件路径，eg：zhangbao.properties
     * @return
     */
    public static Config Load(String pro){
        if(StrUtil.isEmpty(pro)){
            config = ConfigFactory.load("zhangbao.properties");
        }else {
            config = ConfigFactory.load(pro);
        }
        return config;
    }

    public static String getKey(String key){
        if(config == null){
            config = ConfigFactory.load("zhangbao.properties");
        }
        return config.getString(key).trim();
    }
    public static String getKey(String key,String pro){
        Config config = ConfigFactory.load(pro);
        return config.getString(key).trim();
    }
}
