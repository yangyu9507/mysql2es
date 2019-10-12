package com.gy.utils.esapi;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * created by yangyu on 2019-10-10
 */
@Service
public class DocumentApis {

    private static final Logger logger = LoggerFactory.getLogger(DocumentApis.class);
    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;

    @Autowired
    private RestHighLevelClient client;


    public String indexByJson(String index,String id,String jsonString) throws IOException {
        IndexRequest request = new IndexRequest();
        Preconditions.checkArgument(StringUtils.isNotBlank(index),"index must not be null. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(jsonString),"jsonString must not be null. ");

        request.index(index);
        request.source(jsonString, XContentType.JSON);
        if (StringUtils.isNotBlank(id)){
            request.id(id);
        }
        IndexResponse response = client.index(request, DEFAULT_REQUEST_OPTIONS);
        return Strings.toString(response,true,false);
    }

    public String indexByMap(String index,String id,Map<String,Object> map) throws IOException {
        IndexRequest request = new IndexRequest();
        Preconditions.checkArgument(StringUtils.isNotBlank(index),"index must not be null. ");
        Preconditions.checkArgument(Objects.nonNull(map) && 0 != map.size(),"map must not be null. ");

        request.index(index);
        request.source(map);
        if (StringUtils.isNotBlank(id)){
            request.id(id);
        }
        IndexResponse response = client.index(request, DEFAULT_REQUEST_OPTIONS);

        if (response.getResult() == DocWriteResponse.Result.CREATED){

        }else if (response.getResult() == DocWriteResponse.Result.UPDATED) {

        }
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        StringBuffer failureInfo = new StringBuffer();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    failureInfo.append(failure.reason()).append(System.lineSeparator());
                }
            }
        }
        String errorMsg = failureInfo.toString();

        return Strings.toString(response,true,false);
    }

    public Map<String, Object> getDoc(String index,String[] includes,String[] excludes,String id) throws IOException{
        GetRequest request = new GetRequest();
        request.index(index);
        request.id(id);
        if (Objects.isNull(includes) || 0 == includes.length){
            includes = Strings.EMPTY_ARRAY;
        }
        if (Objects.isNull(excludes) || 0 == excludes.length){
            excludes = Strings.EMPTY_ARRAY;
        }

        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        GetResponse response = client.get(request, DEFAULT_REQUEST_OPTIONS);
        return response.getSourceAsMap();
    }

    public boolean existDoc(String index,String id) throws IOException{
        GetRequest request = new GetRequest();
        request.index(index);
        request.id(id);
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");

        return client.exists(request, DEFAULT_REQUEST_OPTIONS);
    }

    public boolean deleteDocById(String index,String id) throws IOException {
        DeleteRequest request = new DeleteRequest();
        request.index(index);
        request.id(id);
        DeleteResponse response = client.delete(request, DEFAULT_REQUEST_OPTIONS);

        return response.getResult() == DocWriteResponse.Result.DELETED;
    }

    public boolean updateDocByScript(String index,String id,Map<String,Object> parameters) throws IOException {
        UpdateRequest request = new UpdateRequest();
        request.index(index);
        request.id(id);

        StringBuffer codeStr = new StringBuffer();
        Iterator<String> iterator = parameters.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            codeStr.append(String.format("ctx._source.%s = params.%s;",key,key));
        }

        Script inline = new Script(ScriptType.INLINE, "painless",codeStr.toString(), parameters);

        request.script(inline);
        request.retryOnConflict(3);

        UpdateResponse updateResponse = client.update(request, DEFAULT_REQUEST_OPTIONS);

        return updateResponse.getResult() == DocWriteResponse.Result.UPDATED;
    }

    public boolean updateDocByQuery(QueryBuilder query, Map<String,Object> parameters,String... indices) throws IOException {

        UpdateByQueryRequest request = new UpdateByQueryRequest(indices);
        request.setConflicts("proceed");

        request.setQuery(query);
        request.setBatchSize(1000);
        // Refresh index after calling update by query
        request.setRefresh(true);

        StringBuffer codeStr = new StringBuffer();
        Iterator<String> iterator = parameters.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            codeStr.append(String.format("ctx._source.%s = params.%s;",key,key));
        }

        request.setScript(new Script(ScriptType.INLINE, "painless",codeStr.toString(),parameters));
        BulkByScrollResponse response = client.updateByQuery(request, DEFAULT_REQUEST_OPTIONS);

        return response.getTotal() == response.getUpdated();
    }



    public boolean DeleteDocByQuery(QueryBuilder query, Map<String,Object> parameters,String... indices) throws IOException {

        DeleteByQueryRequest request = new DeleteByQueryRequest(indices);
        request.setConflicts("proceed");

        request.setQuery(query);
        request.setBatchSize(1000);
        // Refresh index after calling update by query
        request.setRefresh(true);

        StringBuffer codeStr = new StringBuffer();
        Iterator<String> iterator = parameters.keySet().iterator();
        while (iterator.hasNext()){
            String key = iterator.next();
            codeStr.append(String.format("ctx._source.%s = params.%s;",key,key));
        }

        BulkByScrollResponse response = client.deleteByQuery(request, DEFAULT_REQUEST_OPTIONS);

        return response.getTotal() == response.getDeleted();
    }



}
