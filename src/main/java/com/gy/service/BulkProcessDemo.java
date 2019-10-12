package com.gy.service;

import com.gy.entity.ImportTaskEntity;
import gy.lib.common.util.NumberUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * created by yangyu on 2019-09-17
 */
@Service
public class BulkProcessDemo {

    private static final Logger logger = LogManager.getLogger(BulkProcessDemo.class);

    /**
     * 将mysql 数据查出组装成es需要的map格式，通过批量写入es中
     *
     * @param taskEntity
     * @param bulkProcessor
     */
    public void writeMysqlDataToES(ImportTaskEntity taskEntity,BulkProcessor bulkProcessor,Connection conn) {

        String tableName = taskEntity.getTableName();
        String sql = taskEntity.getSql();
        String index = taskEntity.getIndex();
        String type = taskEntity.getType();
        Boolean removeEmpty = taskEntity.getRemoveEmpty();
        // 别名前缀
        String aliasPrefix = null;
        String aliasKey = null;
        boolean isMixed = false;
        if (StringUtils.isNotEmpty(taskEntity.getAliasPrefix())) {
            aliasPrefix = taskEntity.getAliasPrefix();
            aliasKey = StringUtils.substringBefore(aliasPrefix,"_");
            isMixed = true;
        }
        String primaryKey = null;
        if (StringUtils.isNotEmpty(taskEntity.getPrimaryKey())) {
            primaryKey = taskEntity.getPrimaryKey();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {

            logger.info("Start handle data :" + tableName);

//            ps = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ps.setFetchSize(Integer.MIN_VALUE);
            rs = ps.executeQuery();

            ResultSetMetaData colData = rs.getMetaData();

            ArrayList<HashMap<String, Object>> dataList = new ArrayList<>();

            // bulkProcessor 添加的数据支持的方式并不多，查看其api发现其支持map键值对的方式，故笔者在此将查出来的数据转换成hashMap方式
            HashMap<String, Object> map = null;
            int count = 0;
            String c = null;

            if (!isMixed) {
                for (int i = 1; i <= colData.getColumnCount(); i++){
                    c = colData.getColumnName(i);
                    Object obj = rs.getObject(c);
                    if (Objects.isNull(obj) && removeEmpty) {
                        continue;
                    }

                    int columnType = colData.getColumnType(i);
                    getColumnType(rs, c, c, columnType, map);

                    // 每10万条写一次，不足的批次的最后再一并提交
                    if (count > 0 && count % 100000 == 0) {
                        logger.info("Mysql handle data number : {}", count);
                        // 将数据添加到 bulkProcessor 中
                        for (HashMap<String, Object> hashMap2 : dataList) {
                            bulkProcessor.add(new IndexRequest(index).source(hashMap2));
                        }
                        // 每提交一次便将map与list清空
                        map.clear();
                        dataList.clear();
                        bulkProcessor.flush();
                    }
                }
            } else {
                List<HashMap<String, Object>> skuList = new ArrayList<>();
                Long lastId = Long.MIN_VALUE;
                boolean repeat = false;
                while (rs.next()) {
                    for (int i = 1; i <= colData.getColumnCount(); i++) {
                        c = colData.getColumnName(i);
                        if (StringUtils.equalsIgnoreCase(primaryKey, c)) {
                            Long currentId = rs.getLong(c);
                            repeat = lastId.equals(currentId);
                            lastId = currentId;
                            break;
                        }
                    }

                    // 每10万条写一次，不足的批次的最后再一并提交
                    if (count > 0 && count % 100000 == 0 && (!repeat || rs.isLast())) {
                        logger.info("Mysql handle data number : {}", count);
                        // 将数据添加到 bulkProcessor 中
                        for (HashMap<String, Object> hashMap2 : dataList) {
                            bulkProcessor.add(new IndexRequest(index).source(hashMap2));
                        }
                        // 每提交一次便将map与list清空
                        map.clear();
                        dataList.clear();
                        bulkProcessor.flush();
                    }

                    if (!repeat) {
                        count++;
                        map = new HashMap<>(128);
                        dataList.add(map);
                        skuList = new ArrayList<>();

                        for (int i = 1; i <= colData.getColumnCount(); i++) {
                            String columnLabel = colData.getColumnLabel(i);
                            if (StringUtils.startsWith(columnLabel, aliasPrefix)) {
                                continue;
                            }
                            c = colData.getColumnName(i);
                            Object obj = rs.getObject(c);
                            if (Objects.isNull(obj) && removeEmpty) {
                                continue;
                            }

                            int columnType = colData.getColumnType(i);
                            getColumnType(rs, c, c, columnType, map);
                        }
                    }

                    HashMap<String, Object> skuMap = new HashMap<>();
                    for (int i = 1, len = colData.getColumnCount(); i <= len; i++) {
                        c = colData.getColumnLabel(i);
                        if (!StringUtils.startsWith(c, aliasPrefix)) {
                            continue;
                        }

                        if (Objects.isNull(rs.getObject(c)) && removeEmpty) {
                            continue;
                        }
                        int columnType = colData.getColumnType(i);
                        getColumnType(rs, c, colData.getColumnName(i), columnType, skuMap);
                    }

                    if (Objects.nonNull(skuMap) && 0 != skuMap.size()) {
                        skuList.add(skuMap);
                    }
                    if (CollectionUtils.isNotEmpty(skuList)) {
                        map.put(aliasKey, skuList);
                    }
                }
            }

            // count % 100000 处理未提交的数据
            for (HashMap<String, Object> hashMap2 : dataList) {
                bulkProcessor.add(new IndexRequest(index).source(hashMap2));
            }

            logger.info("-------------------------- Finally insert Index[index:{},type:{}]number total : {}",index,type,count);
            // 将数据刷新到es, 注意这一步执行后并不会立即生效，取决于bulkProcessor设置的刷新时间
            bulkProcessor.flush();

        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (Objects.nonNull(rs)){
                    rs.close();
                }
                if (Objects.nonNull(ps)){
                    ps.close();
                }
            } catch (SQLException ex) {
                logger.error("Closing PreparedStatement or ResultSet Failed:",ex);
            }
        }
    }

    /**
     * Mysql -> ES 数据类型转换
     *
     * @param rs
     * @param c
     * @param columnType
     * @param map
     * @throws SQLException
     */
    private void getColumnType(ResultSet rs, String c,String column,int columnType,HashMap<String,Object> map) throws SQLException{
        switch (columnType) {
            case Types.BIGINT:
                map.put(column,rs.getLong(c));
                break;
            case Types.DECIMAL:
                map.put(column,rs.getDouble(c));
                break;
            case Types.DATE:
                Date date = rs.getDate(c);
                map.put(column,date.getTime() / 1000);
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                Time time = rs.getTime(c);
                map.put(column, time.getTime()/ 1000);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                Timestamp timestamp = rs.getTimestamp(c);
                map.put(column,timestamp.getTime()/ 1000);
                break;
            case Types.INTEGER:
                map.put(column, rs.getInt(c));
                break;
            case Types.TINYINT:
                map.put(column,NumberUtil.toInt(rs.getString(c)));
                break;
            case Types.BIT:
                map.put(column,rs.getInt(c));
                break;
            default:
                map.put(column, rs.getString(c));
                break;
        }
    }

}
