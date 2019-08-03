package com.yuyu.stream.common.utils;

import com.yuyu.stream.spark.model.UserTransactionRequestModel;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class JSONUtilTest {
    @Test
    public void testProcess() throws IOException {
        String message = "{\"userId\":1001,\"day\":\"2019-07-27\",\"begintime\":1564156800000,\"endtime\":1564157400000,\"data\":" +
                "[{\"system\":\"com.browser1\",\"transactionsum\":60000},{\"system\":\"com.browser3\",\"transactionsum\":120000}]}";

        try {
            UserTransactionRequestModel requestModel = JSONUtil.json2Object(message, UserTransactionRequestModel.class);
            Assert.assertEquals(requestModel.getUserId(),1001);
            Assert.assertEquals(requestModel.getBeginTime(), Long.valueOf("1564156800000"));
            Assert.assertEquals(requestModel.getSingleUserTransactionRequestModelList().size(), 2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}