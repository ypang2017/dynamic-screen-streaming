package com.yuyu.stream.spark.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * @author yuyu
 */
public class UserTransactionRequestModel extends BaseModel {
    @JsonProperty("begintime")
    private Long beginTime;

    @JsonProperty("endtime")
    private Long endTime;

    @JsonProperty("data")
    private List<SingleUserTransactionRequestModel> singleUserTransactionRequestModelList;

    private long userId;

    private String day;

    public Long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Long beginTime) {
        this.beginTime = beginTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public List<SingleUserTransactionRequestModel> getSingleUserTransactionRequestModelList() {
        return singleUserTransactionRequestModelList;
    }

    public void setSingleUserTransactionRequestModelList(List<SingleUserTransactionRequestModel> singleSystemTransactionRequestModelList) {
        this.singleUserTransactionRequestModelList = singleUserTransactionRequestModelList;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }
}