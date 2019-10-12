package com.gy.utils.esapi;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.elasticsearch.client.indices.CreateIndexRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * created by yangyu on 2019-10-10
 */
@Service
public class IndexApis {

    private static final Logger logger = LoggerFactory.getLogger(IndexApis.class);

    @Autowired
    private RestHighLevelClient client;

    public boolean createIndex(String indexName,Map<String,Object> settingMap,Map<String,Object> mappingMap,List<String> alias) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        request.settings(settingMap);
        request.mapping(mappingMap);
        request.aliases(alias.stream().map(ali -> new Alias(ali)).collect(Collectors.toList()));

        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        return response.isAcknowledged();
    }

    public boolean deleteIndex(String index) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(index);

        request.indicesOptions(IndicesOptions.fromOptions(
                true, true, true,
                false, IndicesOptions.strictExpandOpenAndForbidClosed()));

        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);

        return response.isAcknowledged();
    }

    public boolean indexExist(String index) throws IOException {
        GetIndexRequest request = new GetIndexRequest(index);
        // 是返回本地信息还是从主节点检索状态
        request.local(false);
        request.humanReadable(true);
        // 是否为每个索引返回所有默认设置
        request.includeDefaults(false);
        // 控制如何解决不可用的索引以及如何扩展通配符表达式
        request.indicesOptions(IndicesOptions.fromOptions(
                true, true, true,
                false, IndicesOptions.strictExpandOpenAndForbidClosed()));

        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

}
