package com.zhangbao.portrait.reduce;


import com.zhangbao.portrait.entity.CarrierInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/18 23:19
 **/
public class CarrierReduce implements ReduceFunction<CarrierInfo> {
    @Override
    public CarrierInfo reduce(CarrierInfo carrierInfo, CarrierInfo t1) throws Exception {
        CarrierInfo c = new CarrierInfo();
        c.setCarrier(carrierInfo.getCarrier());
        c.setCount(c.getCount() + t1.getCount());
        return c;
    }
}
