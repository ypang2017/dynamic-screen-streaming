package com.yuyu.stream.spark.model;

/**
 * @author yuyu
 */
public class UserTransactionAmountModel extends BaseModel {
    private String userId;
    private String hour;
    private String systemName;
    private long amount;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemNameName(String systemName) {
        this.systemName = systemName;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}