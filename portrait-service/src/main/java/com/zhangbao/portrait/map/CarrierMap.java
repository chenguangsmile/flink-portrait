package com.zhangbao.portrait.map;

import com.zhangbao.portrait.entity.CarrierInfo;
import com.zhangbao.portrait.utils.CarrierUtils;
import com.zhangbao.portrait.utils.HBaseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author zhangbao
 * @date 2020/11/15 21:24
 **/
public class CarrierMap implements MapFunction<String, CarrierInfo> {
    @Override
    public CarrierInfo map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] split = s.split(",");
        String userId = split[0];
        String userName = split[1];
        String password = split[2];
        String sex = split[3];
        String telphone = split[4];
        String email = split[5];
        String age = split[6];
        String registerTime = split[7];
        String userType = split[8];//用户类型，0：pc，1：移动，2：小程序
        String tableName = "userFlagInfo";
        String row = userId;
        String columnFamily = "baseInfo";
        String column = "carrierInfo";

        int carrierType = CarrierUtils.getCarrierByTel(telphone);//运营商
        String carrierTypeStr = "";
        switch (carrierType){
            case 1: carrierTypeStr = "移动用户";break;
            case 2: carrierTypeStr = "联通用户";break;
            case 3: carrierTypeStr = "电信用户";break;
            default: carrierTypeStr ="未知运营商";
        }
        //将用户年代信息存入HBase中
        HBaseUtils.putData(tableName,row,columnFamily,column,carrierTypeStr);
        CarrierInfo carrierInfo = new CarrierInfo();
        carrierInfo.setCount(1L);
        carrierInfo.setCarrier(carrierTypeStr);
        carrierInfo.setGroupField("carrierInfo=" + carrierTypeStr);
        return carrierInfo;
    }

}
