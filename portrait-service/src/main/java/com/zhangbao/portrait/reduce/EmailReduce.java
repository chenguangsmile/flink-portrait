package com.zhangbao.portrait.reduce;


import com.zhangbao.portrait.entity.EmailInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author zhangbao
 * @date 2020/11/18 23:19
 **/
public class EmailReduce implements ReduceFunction<EmailInfo> {

    @Override
    public EmailInfo reduce(EmailInfo emailInfo, EmailInfo t1) throws Exception {
        EmailInfo email = new EmailInfo();
        email.setEmailType(emailInfo.getEmailType());
        email.setCount(emailInfo.getCount() + t1.getCount());
        return email;
    }
}
