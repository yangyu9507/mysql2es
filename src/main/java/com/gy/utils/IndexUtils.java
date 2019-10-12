package com.gy.utils;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zxp.esclientrhl.util.IndexTools;
import org.zxp.esclientrhl.util.MappingData;
import org.zxp.esclientrhl.util.MetaData;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * created by yangyu on 2019-09-17
 */
@Service
public class IndexUtils<T> {

    private static final Logger logger = LoggerFactory.getLogger(IndexUtils.class);

    @Autowired
    private RestHighLevelClient client;

    public boolean createIndexWithJavaClass(Class<T> clazz) throws Exception{
        MetaData metaData = IndexTools.getMetaData(clazz);

        boolean exists = exists(metaData.getIndexname(),metaData.getIndextype());
        if (exists){
            logger.warn("Index [index:{},type:{}] has already existed, can't create again! ",metaData.getIndexname(),metaData.getIndextype());
            return false;
        }

        CreateIndexRequest request = new CreateIndexRequest(metaData.getIndexname());

        StringBuffer source = new StringBuffer();
        source.append("  {\n" +
                "    \""+metaData.getIndextype()+"\": {\n" +
                   "\"_all\": {\n"+
                 "\"enabled\": false\n" +
                "},\n"+
                "      \"properties\": {\n");
        MappingData[] mappingDataList = IndexTools.getMappingData(clazz);

        boolean isAutocomplete = false;
        for (int i = 0; i < mappingDataList.length; i++) {
            MappingData mappingData = mappingDataList[i];
            if(Objects.isNull(mappingData) || StringUtils.isEmpty(mappingData.getField_name())){
                continue;
            }
            source.append(" \""+mappingData.getField_name()+"\": {\n");
            source.append(" \"type\": \""+mappingData.getDatatype()+"\"\n");
            if(!StringUtils.isEmpty(mappingData.getCopy_to())){
                source.append(" ,\"copy_to\": \""+mappingData.getCopy_to()+"\"\n");
            }
            if(!mappingData.isAllow_search()){
                source.append(" ,\"index\": false\n");
            }
            if(mappingData.isAutocomplete() && (mappingData.getDatatype().equals("text") || mappingData.getDatatype().equals("keyword"))){
                source.append(" ,\"analyzer\": \"autocomplete\"\n");
                source.append(" ,\"search_analyzer\": \"standard\"\n");
                isAutocomplete = true;
            }else if(mappingData.getDatatype().equals("text")){
                source.append(" ,\"analyzer\": \"" + mappingData.getAnalyzer() + "\"\n");
                source.append(" ,\"search_analyzer\": \"" + mappingData.getSearch_analyzer() + "\"\n");
            }

            if(mappingData.isKeyword() && !mappingData.getDatatype().equals("keyword") && mappingData.isSuggest()){
                source.append(" \n");
                source.append(" ,\"fields\": {\n");

                source.append(" \"keyword\": {\n");
                source.append(" \"type\": \"keyword\",\n");
                source.append(" \"ignore_above\": "+mappingData.getIgnore_above());
                source.append(" },\n");

                source.append(" \"suggest\": {\n");
                source.append(" \"type\": \"completion\",\n");
                source.append(" \"analyzer\": \""+mappingData.getAnalyzer()+"\"\n");
                source.append(" }\n");

                source.append(" }\n");
            }else if(mappingData.isKeyword() && !mappingData.getDatatype().equals("keyword") && !mappingData.isSuggest()){
                source.append(" \n");
                source.append(" ,\"fields\": {\n");
                source.append(" \"keyword\": {\n");
                source.append(" \"type\": \"keyword\",\n");
                source.append(" \"ignore_above\": "+mappingData.getIgnore_above());
                source.append(" }\n");
                source.append(" }\n");
            }else if(!mappingData.isKeyword() && mappingData.isSuggest()){
                source.append(" \n");
                source.append(" ,\"fields\": {\n");
                source.append(" \"suggest\": {\n");
                source.append(" \"type\": \"completion\",\n");
                source.append(" \"analyzer\": \""+mappingData.getAnalyzer()+"\"\n");
                source.append(" }\n");
                source.append(" }\n");
            }
            if(i == mappingDataList.length - 1){
                source.append(" }\n");
            }else{
                source.append(" },\n");
            }
        }
        source.append(" }\n");
        source.append(" }\n");
        source.append(" }\n");

        if(isAutocomplete){
            request.settings(Settings.builder()
                    .put("index.number_of_shards", metaData.getNumber_of_shards())
                    .put("index.number_of_replicas", metaData.getNumber_of_replicas())
                    .put("analysis.filter.autocomplete_filter.type","edge_ngram")
                    .put("analysis.filter.autocomplete_filter.min_gram",1)
                    .put("analysis.filter.autocomplete_filter.max_gram",20)
                    .put("analysis.analyzer.autocomplete.type","custom")
                    .put("analysis.analyzer.autocomplete.tokenizer","standard")
                    .putList("analysis.analyzer.autocomplete.filter",new String[]{"lowercase","autocomplete_filter"})
            );
        }else{
            request.settings(Settings.builder()
                    .put("index.number_of_shards", metaData.getNumber_of_shards())
                    .put("index.number_of_replicas", metaData.getNumber_of_replicas())
                    .put("index.refresh_interval",metaData.getRefresh_interval())
            );

        }

        request.mapping(metaData.getIndextype(),//类型定义
                source.toString(),//类型映射，需要的是一个JSON字符串
                XContentType.JSON);

        boolean acknowledged = false;
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            acknowledged = createIndexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            return acknowledged;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return acknowledged;
    }

    public boolean dropIndex(String...indices) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indices);
        AcknowledgedResponse result = client.indices().delete(request, RequestOptions.DEFAULT);
        return result.isAcknowledged();
    }

    public boolean exists(String index,String type) throws IOException{
        if (StringUtils.isEmpty(index)){
            return false;
        }
        GetIndexRequest request = new GetIndexRequest();
        request.indices(index);
        if (StringUtils.isNotEmpty(type)){
            request.types(type);
        }
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    public boolean updateSetting(String index,Map<String,String> paramsMap) throws IOException {
        UpdateSettingsRequest request = new UpdateSettingsRequest();
        request.indices(index);
        Settings.Builder settings = Settings.builder();
        for(Map.Entry<String,String> settingEntry : paramsMap.entrySet()) {
            settings.put(settingEntry.getKey(),settingEntry.getValue());
        }
        request.settings(settings.build());
        AcknowledgedResponse acknowledgedResponse = client.indices().putSettings(request, RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }

}
