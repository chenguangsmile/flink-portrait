package com.zhangbao.entity;


import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangbao
 * @date 2020/11/22 0:11
 **/
public class ResultData extends HashMap<String, Object> {
    private static final long serialVersionUID = 1L;


    public ResultData() {
        put("status", 0);
        put("msg", "操作成功");
    }

    public static ResultData error() {
        return error(1, "操作失败");
    }

    public static ResultData error(String msg) {
        return error(500, msg);
    }

    public static ResultData error(int code, String msg) {
        ResultData resultData = new ResultData();
        resultData.put("code", code);
        Object msgs = null;
        if(msgs!=null){
            resultData.put("msg", msgs);
        }else{
            resultData.put("msg", msg);
        }
        return resultData;
    }

    public static ResultData ok(String msg) {
        ResultData resultData = new ResultData();
        Object msgs = null;
        if(msgs!=null){
            resultData.put("msg", msgs);
        }else{
            resultData.put("msg", msg);
        }
        return resultData;
    }

    public static ResultData ok(Map<String, Object> map) {
        ResultData resultData = new ResultData();
        resultData.putAll(map);
        return resultData;
    }
    public static ResultData ok(Object list,Object count) {
        ResultData resultData = new ResultData();
        resultData.put("data", list);
        resultData.put("count", count);
        return resultData;
    }
    public static ResultData ok(String key,Object value) {
        ResultData resultData = new ResultData();
        resultData.put(key, value);
        return resultData;
    }
    public static ResultData ok(int code, String msg,String data) {
        ResultData resultData = new ResultData();
        resultData.put("code", code);
        resultData.put("msg", msg);
        resultData.put("data", data);
        return resultData;
    }
    public static ResultData ok(int code, String data) {
        ResultData resultData = new ResultData();
        resultData.put("code", code);
        resultData.put("data", data);
        return resultData;
    }

    public static ResultData ok() {
        return new ResultData();
    }


    public static ResultData ok(Object object) {
        ResultData resultData = new ResultData();
        resultData.put("code", "0");
        resultData.put("msg", "数据获取成功");
        resultData.put("data", object);
        return resultData;
    }
    @Override
    public ResultData put(String key, Object value) {
        super.put(key, value);
        return this;
    }
}
