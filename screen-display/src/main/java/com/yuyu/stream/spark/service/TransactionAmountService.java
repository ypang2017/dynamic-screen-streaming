package com.yuyu.stream.spark.service;


import com.yuyu.stream.spark.model.UserTransactionAmountModel;


/**
 * @author yuyu
 */

public interface TransactionAmountService {
  /**
  * 交易金额统计
  */
  public void addTransactionSum(UserTransactionAmountModel model) throws Exception;

  /**
  * 用户每天每小时的通过系统的交易金额
  */
  public void addUserHourAmount(UserTransactionAmountModel model) throws Exception;

  /**
  * 用户每天的交易金额
  */
  public void addUserDayAmount(UserTransactionAmountModel model) throws Exception;

  /**
  * 用户每个系统每小时的交易金额
  */
  public void addUserSystemHourAmount(UserTransactionAmountModel model) throws Exception;

  /**
  * 用户每个系统每天的交易金额
  */
  public void addUserSystemDayAmount(UserTransactionAmountModel model) throws Exception;
}
