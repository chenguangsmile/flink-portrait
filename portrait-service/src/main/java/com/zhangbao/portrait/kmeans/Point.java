package com.zhangbao.portrait.kmeans;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class Point {
    private float[] localArray;
    private int id;
    private int clusterId;  // 标识属于哪个类中心。
    private float dist;     // 标识和所属类中心的距离。
    private Point clusterPoint;//中心点信息
 
    public Point(int id, float[] localArray) {
        this.id = id;
        this.localArray = localArray;
    }
 
    public Point(float[] localArray) {
        this.id = -1; //表示不属于任意一个类
        this.localArray = localArray;
    }

 
    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
 
    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass())
            return false;
 
        Point point = (Point) obj;
        if (point.localArray.length != localArray.length)
            return false;
 
        for (int i = 0; i < localArray.length; i++) {
            if (Float.compare(point.localArray[i], localArray[i]) != 0) {
                return false;
            }
        }
        return true;
    }
 
    @Override
    public int hashCode() {
        float x = localArray[0];
        float y = localArray[localArray.length - 1];
        long temp = x != +0.0d ? Double.doubleToLongBits(x) : 0L;
        int result = (int) (temp ^ (temp >>> 32));
        temp = y != +0.0d ? Double.doubleToLongBits(y) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
