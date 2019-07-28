package com.yuyu.stream.spark.service.impl;

import com.yuyu.stream.spark.model.UserTransactionAmountModel;
import org.junit.Test;

public class MysqlTransactionAmountServiceTest {
    @Test
    public void testProcess(){
        MysqlTransactionAmountService service = new MysqlTransactionAmountService();
        UserTransactionAmountModel model = new UserTransactionAmountModel();
        model.setUserId("1001");
        model.setSystemNameName("com.browser1");
        model.setHour("2019072810");
        model.setAmount(1000);
        try {
            service.addTransactionSum(model);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}