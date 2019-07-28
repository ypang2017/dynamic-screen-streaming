package com.yuyu.stream.spark.model;

/**
 * @author yuyu
 */
public class UserTransactionModel extends BaseModel {
    private long userId;
    private long beginTime;
    private long endTime;
    private String hour;
    private String onlyHour;
    private String systemName;
    /**
     * 金额
     */
    private long amount;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getOnlyHour() {
        return onlyHour;
    }

    public void setOnlyHour(String onlyHour) {
        this.onlyHour = onlyHour;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}