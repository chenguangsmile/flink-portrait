package com.zhangbao.portrait.reduce;

import com.zhangbao.portrait.entity.BrandLike;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/23 23:56
 **/
public class BrandLikeReduce implements ReduceFunction<BrandLike> {
    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike t1) throws Exception {
        BrandLike finalRankLike = new BrandLike();
        finalRankLike.setBrand(brandLike.getBrand());
        finalRankLike.setCount(brandLike.getCount() + t1.getCount());
        return finalRankLike;
    }
}
