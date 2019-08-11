package com.yuyu.stream.spark.service;

import com.yuyu.stream.spark.dao.impl.MysqlConsumeOffsetDao;
import com.yuyu.stream.spark.model.ConsumeOffsetModel;

import java.util.List;

/**
 * @author yuyu
 */
public class ConsumeOffsetService {
    private MysqlConsumeOffsetDao offsetDao = new MysqlConsumeOffsetDao();

    public void insertOffset(ConsumeOffsetModel model) throws Exception {
        String tableName = "consume_offset";
        String sql = String.format("insert into consume_offset(groupid,topic,partitionid,offset) values(?,?,?,?) ON DUPLICATE KEY UPDATE offset=?", tableName);
        String[] param = new String[]{model.getGroupId(), model.getTopic(), model.getPartition(), model.getOffset(), model.getOffset()};
        offsetDao.insertSQL(sql, param);
    }

    public List<ConsumeOffsetModel> getList() throws Exception {
        String tableName = "consume_offset";
        String sql = String.format("select * from %s;", tableName);
        String[] param = new String[]{};
       return offsetDao.getOffsetInfo(sql, param);
    }
}
