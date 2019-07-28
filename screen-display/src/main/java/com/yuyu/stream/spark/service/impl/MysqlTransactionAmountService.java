package com.yuyu.stream.spark.service.impl;

import com.yuyu.stream.common.utils.DateUtils;
import com.yuyu.stream.spark.dao.impl.MysqlUserTransactionAmountDao;
import com.yuyu.stream.spark.model.UserTransactionAmountModel;
import com.yuyu.stream.spark.service.TransactionAmountService;

/**
 * @author yuyu
 */
public class MysqlTransactionAmountService implements TransactionAmountService {
    private MysqlUserTransactionAmountDao mysqlDao = new MysqlUserTransactionAmountDao();

    @Override
    public void addTransactionSum(UserTransactionAmountModel model) throws Exception {
        addUserHourAmount(model);
        addUserDayAmount(model);
        addUserSystemHourAmount(model);
        addUserSystemDayAmount(model);
    }

    @Override
    public void addUserHourAmount(UserTransactionAmountModel model) throws Exception {
        String tableName = "transaction_user_hour";
        String sql = String.format("insert into %s(user_id,hour,day,amount) values(?,?,?,?)", tableName);
        String[] param = new String[]{model.getUserId(), DateUtils.getOnlyHourByHour(model.getHour()), DateUtils.getOnlyDayByHour(model.getHour()), String.valueOf(model.getAmount())};
        mysqlDao.addUserTransactionAmount(sql, param);
    }

    @Override
    public void addUserDayAmount(UserTransactionAmountModel model) throws Exception {
        String tableName = "transaction_user_day";
        String sql = String.format("insert into %s(user_id,day,amount) values(?,?,?)", tableName);
        String[] param = new String[]{model.getUserId(), DateUtils.getOnlyDayByHour(model.getHour()), String.valueOf(model.getAmount())};
        mysqlDao.addUserTransactionAmount(sql, param);
    }

    @Override
    public void addUserSystemHourAmount(UserTransactionAmountModel model) throws Exception {
        String tableName = "transaction_user_system_hour";
        String sql = String.format("insert into %s(user_id,hour,day,system,amount) values(?,?,?,?,?)", tableName);
        String[] param = new String[]{model.getUserId(), DateUtils.getOnlyHourByHour(model.getHour()), DateUtils.getOnlyDayByHour(model.getHour()),
                model.getSystemName(), String.valueOf(model.getAmount())};
        mysqlDao.addUserTransactionAmount(sql, param);
    }

    @Override
    public void addUserSystemDayAmount(UserTransactionAmountModel model) throws Exception {
        String tableName = "transaction_user_system_day";
        String sql = String.format("insert into %s(user_id,day,system,amount) values(?,?,?,?)", tableName);
        String[] param = new String[]{model.getUserId(), DateUtils.getOnlyDayByHour(model.getHour()), model.getSystemName(), String.valueOf(model.getAmount())};
        mysqlDao.addUserTransactionAmount(sql, param);
    }
}
