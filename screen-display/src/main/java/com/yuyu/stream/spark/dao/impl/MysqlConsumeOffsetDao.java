package com.yuyu.stream.spark.dao.impl;

import com.yuyu.stream.spark.dao.BaseDao;
import com.yuyu.stream.spark.model.ConsumeOffsetModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuyu
 */
public class MysqlConsumeOffsetDao extends BaseDao {

    public List<ConsumeOffsetModel> getOffsetInfo(String sql, String[] param) throws ClassNotFoundException, SQLException {
//        ConsumeOffsetModel model = new ConsumeOffsetModel();
        ConsumeOffsetModel model = null;
        List<ConsumeOffsetModel> list = new ArrayList<>();

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet res = null;
        /* 处理SQL,执行SQL */
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            if (param != null) {
                for (int i = 0; i < param.length; i++) {
                    // 为预编译sql设置参数
                    pstmt.setObject(i + 1, param[i]);
                }
            }
            res = pstmt.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            while (res.next()) {
                model = new ConsumeOffsetModel();
                model.setGroupId(res.getString("groupid"));
                model.setTopic(res.getString("topic"));
                model.setPartition(res.getString("partitionid"));
                model.setOffset(res.getString("offset"));
                list.add(model);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            BaseDao.closeAll(conn, pstmt, res);
        }

        return list;
    }
}
