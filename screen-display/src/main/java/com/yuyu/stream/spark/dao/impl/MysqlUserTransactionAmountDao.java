package com.yuyu.stream.spark.dao.impl;

import com.yuyu.stream.spark.dao.BaseDao;
import com.yuyu.stream.spark.dao.IUserTransactionAmountDao;
import com.yuyu.stream.spark.model.UserTransactionAmountModel;
import java.util.List;

/**
 * @author yuyu
 */
public class MysqlUserTransactionAmountDao extends BaseDao implements IUserTransactionAmountDao{

    @Override
    public List<UserTransactionAmountModel> searchUserTransactionAmount() throws Exception {
        return null;
    }

    @Override
    public void addUserTransactionAmount(String sql, String[] param) throws Exception {
        insertSQL(sql, param);
    }
}
