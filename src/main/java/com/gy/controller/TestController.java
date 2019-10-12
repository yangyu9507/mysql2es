package com.gy.controller;

import com.gy.utils.esapi.ClusterApis;
import com.gy.utils.esapi.DocumentApis;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * created by yangyu on 2019-10-10
 */
@RestController
@RequestMapping(value = "/test")
public class TestController {

    @Autowired
    private ClusterApis clusterApis;

    @Autowired
    private DocumentApis documentApis;

    @GetMapping(value = "/cluster_health")
    public String clusterHealth() throws Exception {
        String clusterHealth = clusterApis.clusterHealth("itemwithsku3");
        return clusterHealth;
    }

    @GetMapping(value = "/cluster_info")
    public String clusterInfo() throws Exception {
        String clusterHealth = clusterApis.info();
        return clusterHealth;
    }



    @GetMapping(value = "/cluster_get_setting")
    public String clusterGetSetting() throws Exception {
        String clusterGetSetting = clusterApis.clusterGetSetting();
        return clusterGetSetting;
    }

    @GetMapping(value = "/cluster_update_setting")
    public boolean clusterUpdateSetting() throws Exception {

        Map<String,Object> transientSettingMap = new HashMap<>();
        transientSettingMap.put("indices.recovery.max_bytes_per_sec", "40m");
        boolean clusterUpdateSetting = clusterApis.clusterUpdateSetting(transientSettingMap,null);
        return clusterUpdateSetting;
    }

    @GetMapping(value = "/index")
    public String index() throws Exception {
//        String index = documentApis.indexByJson("logstash1", "", "{\"name1\":\"sfasfsadf\",\"id1\":6294}");
        String index = "datetest";
        Map<String,Object> map = new HashMap<>();
        map.put("create_date",System.currentTimeMillis() / 1000);
        System.out.println(documentApis.indexByMap(index, "", map));
        map.put("create_date", LocalDateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println(documentApis.indexByMap(index, "", map));
        map.put("create_date", LocalDateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd")));
        System.out.println(documentApis.indexByMap(index, "", map));
        return "";
    }

    @GetMapping(value = "/delete_by_id")
    public boolean delete(@RequestParam(value = "index")String index,@RequestParam(value = "id")String id) throws IOException {
        return documentApis.deleteDocById(index,id);
    }


    @GetMapping(value = "/update")
    public boolean update(@RequestParam(value = "index")String index,@RequestParam(value = "id")String id) throws IOException {
        Map<String,Object> map = new LinkedHashMap<>();
        map.put("name456","name456ojbk");
        return documentApis.updateDocByScript(index, id, map);
    }

    @GetMapping(value = "/update_by_query")
    public boolean updateByQuery(@RequestParam(value = "index")String index) throws IOException {
        Map<String,Object> map = new LinkedHashMap<>();
        map.put("id",324324);
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter
                (QueryBuilders.termQuery("name2","ojbk"));
        return documentApis.updateDocByQuery(query,map,index);
    }


}
