package com.gy.utils.esapi;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * created by yangyu on 2019-10-10
 */
@Service
public class ClusterApis {

    private static final Logger logger = LoggerFactory.getLogger(ClusterApis.class);
    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;

    @Autowired
    private RestHighLevelClient client;

    /**
     * <p>获取集群健康状态</p>
     *
     * @param indices 索引名称
     * @return 集群健康状态JSON描述
     * @throws IOException
     */
    public String clusterHealth (String... indices) throws IOException{
        ClusterHealthRequest request = new ClusterHealthRequest();
        request.indices(indices);
        request.timeout(TimeValue.timeValueSeconds(30));
        request.masterNodeTimeout(TimeValue.timeValueSeconds(30));
        ClusterHealthResponse response = client.cluster().health(request, RequestOptions.DEFAULT);
        return JSONObject.toJSONString(response);
    }

    /**
     * <p>获取集群设置</p>
     * @return 集群响应
     * @throws IOException
     */
    private ClusterGetSettingsResponse clusterGetSettingCommon() throws IOException {
        ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
        request.includeDefaults(true);
        return client.cluster().getSettings(request, RequestOptions.DEFAULT);
    }

    /**
     * <p>获取集群所有配置</p>
     * @return 集群配置详情
     * @throws IOException
     */
    public String clusterGetSetting() throws IOException {
        ClusterGetSettingsResponse settings = clusterGetSettingCommon();
        return Strings.toString(settings,true,false);
    }

    /**
     * <p>获取集群指定配置</p>
     * <link> https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.3/java-rest-high-cluster-put-settings.html </link>
     *
     * @param key 配置名称 eg: cluster.routing.allocation.enable
     * @return    配置详情
     * @throws IOException
     */
    public String clusterGetSettingByKey(String key) throws IOException{
         if (StringUtils.isBlank(key)){
             return "";
         }
         return clusterGetSettingCommon().getSetting(key);
    }

    public boolean clusterUpdateSetting(Map<String,Object> transientSettingMap,Map<String,Object> persistentSettingMap) throws IOException {
        Preconditions.checkArgument(Objects.nonNull(transientSettingMap) || Objects.nonNull(persistentSettingMap),
                      "At least one setting [transientSettings or persistentSettings] to be updated must be provided! ");

        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        if (Objects.nonNull(transientSettingMap) && 0 != transientSettingMap.size()) {
            request.transientSettings(transientSettingMap);
        }
        if (Objects.nonNull(persistentSettingMap) && 0 != persistentSettingMap.size()) {
            request.persistentSettings(persistentSettingMap);
        }
        ClusterUpdateSettingsResponse response = client.cluster().putSettings(request, RequestOptions.DEFAULT);
        if (Objects.isNull(response) || Objects.isNull(response.isAcknowledged())){
            return false;
        }
        return response.isAcknowledged();
    }

    public String info() throws IOException {
        MainResponse response = client.info(RequestOptions.DEFAULT);
        return JSONObject.toJSONString(response);
    }

}
