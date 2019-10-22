package com.gy.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gy.entity.ImportTaskEntity;
import com.gy.service.BulkProcessDemo;
import com.gy.utils.BulkProcessorUtils;
import com.gy.utils.DBHelper;
import com.gy.utils.FileUtils;
import com.gy.utils.IndexUtils;
import gy.lib.common.util.FinanceUtil;
import gy.lib.common.util.NumberUtil;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.units.qual.A;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * created by yangyu on 2019-09-17
 */
@RestController
@RequestMapping(value = "/import")
public class ImportController {

    private static final Logger logger = LoggerFactory.getLogger(ImportController.class);

    @Autowired
    private BulkProcessDemo bulkProcessDemo;
    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private IndexUtils indexUtils;

    @RequestMapping(value = "/table", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void importBySql(@RequestBody Map<String,Object> map) throws Exception{

        BulkProcessor bulkProcessor = BulkProcessorUtils.getInstance(client);

        List<ImportTaskEntity> taskEntityList = JSONArray.parseArray(JSONObject.toJSONString(map.get("taskEntity")), ImportTaskEntity.class);
        Map<String, List<ImportTaskEntity>> taskListMap = taskEntityList.stream().collect(Collectors.groupingBy(ImportTaskEntity::getDbName));

        for (Map.Entry<String, List<ImportTaskEntity>> entry : taskListMap.entrySet()){
            String dbName = entry.getKey();
            List<ImportTaskEntity> importTaskList = entry.getValue();

            Connection conn = getCurrentConnection(dbName);
            if (Objects.isNull(conn))
                continue;

            for (ImportTaskEntity taskEntity : importTaskList) {
                if (StringUtils.isEmpty(taskEntity.getSql())){
                    String sql = FileUtils.readSqlFromFile(taskEntity.getSqlPath());
                    taskEntity.setSql(sql);
                }
                logger.info("Import Mysql [{}] to ES [{}] with Sql[{}{}{}{}{}{}] start! ",
                        taskEntity.getDbName(), taskEntity.getIndex(),  System.lineSeparator(),lineStart,System.lineSeparator(),taskEntity.getSql(),lineEnd,System.lineSeparator());
                long start = System.currentTimeMillis();
                bulkProcessDemo.writeMysqlDataToES(taskEntity,bulkProcessor,conn);
                String executeTime = getExecuteTime(start);
                logger.info("Import Mysql [{}] to ES [index:{}] with Sql[{}{}{}{}{}{}] finished, executeTime:{}",
                        taskEntity.getDbName(), taskEntity.getIndex(), System.lineSeparator(),lineStart,System.lineSeparator(),taskEntity.getSql(),lineEnd,System.lineSeparator(),executeTime);
            }
            closeConnection(conn);
        }
        closeBulkProcessor(bulkProcessor);
    }

    private static final String lineStart = "************************************************ SQL Start **************************************************";
    private static final String lineEnd   = "************************************************ SQL  End  **************************************************";

    private Connection getCurrentConnection(String dbName) throws Exception{
      return DBHelper.getConnection(dbName);
    }

    private void closeConnection(Connection conn){
        try {
            if (Objects.nonNull(conn)) {
                conn.close();
            }
        }catch (SQLException ex){
            logger.error("Closing DB[{}] Connection Failed: ",ex);
        }
    }

    private void closeBulkProcessor(BulkProcessor bulkProcessor) {
        try {
            if(bulkProcessor.awaitClose(150L, TimeUnit.SECONDS)){
                logger.info("Closing BulkProcessor successfully! ");
            }
        }catch (InterruptedException ex) {
            logger.error("Closing BulkProcessor Failed: ",ex);
        }
    }

    /**
     * 计算执行时间
     *
     * @param start
     * @return
     */
    private String getExecuteTime(long start){
        Double totalTime = FinanceUtil.divide(NumberUtil.toDouble(System.currentTimeMillis() - start), NumberUtil.toDouble(1000));
        int hour  = NumberUtil.toInt(Math.floor(totalTime / 3600));
        totalTime %= 3600;
        int min  = NumberUtil.toInt(Math.floor(totalTime / 60));
        String sec  = String.format("%.3f",totalTime % 60);
        return String.format("%dh %dm %ss",hour,min,sec);
    }

}
