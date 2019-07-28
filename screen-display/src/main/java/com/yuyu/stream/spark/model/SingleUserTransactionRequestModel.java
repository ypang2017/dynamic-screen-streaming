package com.yuyu.stream.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author yuyu
 */
public class SingleUserTransactionRequestModel {
    private static final long serialVersionUID = 1L;

    @JsonProperty("system")
    private String systemName;

    @JsonProperty("transationsum")
    private long transationSum;

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public long getTransationSum() {
        return transationSum;
    }

    public void setTransationSum(long transationSum) {
        this.transationSum = transationSum;
    }
}
