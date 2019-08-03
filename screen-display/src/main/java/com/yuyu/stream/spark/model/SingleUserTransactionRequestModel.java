package com.yuyu.stream.spark.model;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * @author yuyu
 */
public class SingleUserTransactionRequestModel {
    private static final long serialVersionUID = 1L;

    @JsonProperty("system")
    private String systemName;

    @JsonProperty("transactionsum")
    private long transactionSum;

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public long getTransationSum() {
        return transactionSum;
    }

    public void setTransationSum(long transationSum) {
        this.transactionSum = transationSum;
    }
}
