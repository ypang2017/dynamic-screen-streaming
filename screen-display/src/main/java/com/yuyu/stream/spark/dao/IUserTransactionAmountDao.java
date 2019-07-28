package com.yuyu.stream.spark.dao;

import com.yuyu.stream.spark.model.UserTransactionAmountModel;

import java.util.List;

/**
 * @author yuyu
 */
public interface IUserTransactionAmountDao {
    /**
     * 查询用户交易记录
     */
    public List<UserTransactionAmountModel> searchUserTransactionAmount() throws Exception;


    /**
     * 添加用户交易记录
     */
    public  void addUserTransactionAmount(String sql, String[] param) throws Exception;
}
