package com.gy.utils;

import gy.lib.common.util.NumberUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * created by yangyu on 2019-09-19
 */
public class BulkProcessorUtils {

    private static final Logger logger = LoggerFactory.getLogger(BulkProcessorUtils.class);


    /**
     * 创建bulkProcessor并初始化
     * @return
     */
    public static BulkProcessor getInstance(RestHighLevelClient client) {
        BulkProcessor bulkProcessor = null;
        Properties properties = null;
        try {

            properties = PropertiesLoaderUtils.loadAllProperties("bulkprocessor.properties");
            Integer bulkActions = NumberUtil.toInt(properties.get("bulkActions"));
            Long bulkSize = NumberUtil.toLong(properties.get("bulkSize"));
            Long flushInterval = NumberUtil.toLong(properties.get("flushInterval"));
            Integer concurrentRequestss = NumberUtil.toInt(properties.get("concurrentRequestss"));
            Long backoffTimeSeconds = NumberUtil.toLong(properties.get("backoffTimeSeconds"));
            Integer  maxNumberOfRetries = NumberUtil.toInt(properties.get("maxNumberOfRetries"));


            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    logger.info("Try to insert data number : " + request.numberOfActions());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    logger.info("************** Success insert data number : " + request.numberOfActions() + " , id: "
                            + executionId,", hasFailures :" + response.hasFailures());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    logger.error("Bulk is failed : " + failure + ", executionId: " + executionId);
                }
            };

            BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = (request, bulkListener) -> client
                    .bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

//            bulkProcessor = BulkProcessor.builder(bulkConsumer, listener).build();

            BulkProcessor.Builder builder = BulkProcessor.builder(bulkConsumer, listener);

            // 每10000次请求执行批量处理
            builder.setBulkActions(bulkActions);
            // 每5MB 刷新一次
            builder.setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB));
            // 无论请求数量多少,都希望每隔5秒刷新一次
            builder.setFlushInterval(TimeValue.timeValueSeconds(flushInterval));
            // 设置并发请求数,值为0表示只允许执行单个请求。 值1表示允许执行1个并发请求，同时累积新的批量请求
            builder.setConcurrentRequests(concurrentRequestss);
            // 设置自定义退避策略，该策略最初将等待100毫秒，以指数方式增加并重试最多三次。 每当一个或多个批量项请求失败并且EsRejectedExecutionException指示可用于处理请求的计算资源太少时，就会尝试重试
            builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(backoffTimeSeconds), maxNumberOfRetries));

            // 注意点：在这里感觉有点坑，官网样例并没有这一步，而笔者因一时粗心也没注意，在调试时注意看才发现，上面对builder设置的属性没有生效
            bulkProcessor = builder.build();

        } catch (Exception ex) {
            logger.error("BulkProcessor getInstance failed :",ex);
            try {
                Long awaitClose = NumberUtil.toLong(properties.get("awaitClose"));
                bulkProcessor.awaitClose(awaitClose, TimeUnit.SECONDS);
                client.close();
            } catch (Exception e1) {
                logger.error("BulkProcessor close failed :" , e1);
            }
        }
        return bulkProcessor;
    }
}
