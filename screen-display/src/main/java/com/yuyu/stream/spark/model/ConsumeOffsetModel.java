package com.yuyu.stream.spark.model;

/**
 * @author yuyu
 */
public class ConsumeOffsetModel extends BaseModel {
    private String groupId;
    private String topic;
    private String partitionId;
    private String offset;

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partitionId;
    }

    public void setPartition(String partition) {
        this.partitionId = partition;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }
}
