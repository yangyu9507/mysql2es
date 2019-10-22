package com.gy.controller;

import com.alibaba.fastjson.JSONObject;
import com.gy.utils.ESRequestBuilder;
import com.gy.utils.EsHelper;
import com.gy.utils.IndexUtils;
import com.gy.utils.esapi.SearchApis;
import gy.lib.common.util.JsonUtil;
import gy.lib.common.util.NumberUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.alibaba.fastjson.serializer.SerializerFeature.WriteBigDecimalAsPlain;
import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * created by yangyu on 2019-09-20
 */
@RestController
@RequestMapping(value = "/search")
public class SearchController {

    private static final Logger logger = LoggerFactory.getLogger(SearchController.class);

    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;
    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private IndexUtils indexUtils;
    @Autowired
    private EsHelper esHelper;
    @Autowired
    private SearchApis searchApis;

    private static final long limit = 10000L;
    private static final String index = "item_v2";

    @PostMapping(value = "/wild", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void wildSearch(@RequestBody Map<String,Object> paramsMap) throws Exception{

        BoolQueryBuilder query = boolQuery();
//        query.filter(fuzzyQuery("code",paramsMap.get("code")));

//        query.filter(wildcardQuery("code","*" + String.valueOf(paramsMap.get("code")) + "*"));

//        query.filter(regexpQuery("code","*" + String.valueOf(paramsMap.get("code")) + "*"));

//        query.filter(prefixQuery("code",String.valueOf(paramsMap.get("code"))));

//        query.filter(matchPhraseQuery("code",String.valueOf(paramsMap.get("code"))));

        QueryStringQueryBuilder codeQuery
                = queryStringQuery(String.valueOf(paramsMap.get("code")))
                                  .field("code")
                           .allowLeadingWildcard(true)
                           .analyzeWildcard(true)
                           .minimumShouldMatch("50%");
        query.filter(codeQuery);

        SearchRequest request = ESRequestBuilder.builder()
                 .setIndex(index)
                 .setQuery(query)
                 .build();

        long count = searchApis.count(query, index);
        System.out.println("count : " + count);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHit[] hits = response.getHits().getHits();
        Arrays.stream(hits).map(JSONObject::toJSONString).forEach(System.out::println);

    }

    public ESRequestBuilder.Builder getBuilder (BoolQueryBuilder query){
        ESRequestBuilder.Builder commonBuilder = ESRequestBuilder.builder()
                .setIndex("itemwithsku4")
                .setQuery(query)
                .addSort("create_date", SortOrder.DESC)
                .addSort("_id", SortOrder.DESC)
                .setFetchSource(true)
                .setFetchFields(new String[]{"create_date"});
        return commonBuilder;
    }

    @RequestMapping(value = "/t1", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void search1(@RequestBody Map<String,String> map) throws Exception {

        BoolQueryBuilder query = boolQuery();
        query.filter(termQuery("combine",0));

        int pageIndex = Integer.parseInt(map.get("pageIndex"));
        int pageSize = Integer.parseInt(map.get("pageSize"));

        int start = (pageIndex - 1) * pageSize;
        int end = start + pageSize;

        ESRequestBuilder.Builder builder = getBuilder(query);
        SearchRequest request = null;
        if (end > limit) {
            builder = builder.setFrom(NumberUtil.toInt(limit - 1)).setSize(1);
        } else {
            builder = builder.setFrom(start).setSize(pageSize);
        }
        request= builder.build();
        Long totalCount = esHelper.getTotalCount(query, "itemwithsku4");
        System.out.println(totalCount);
        SearchResponse response = client.search(request, DEFAULT_REQUEST_OPTIONS);
        int startIndex = end > limit ? NumberUtil.toInt(start % limit) : 0;

        if (end > limit) {
            int needPageTurnNum = NumberUtil.toInt(end / limit);
            SearchHit documentFields = null;
            Map<String, Object> sourceAsMap = null;
            Object[] objs = null;

            documentFields = response.getHits().getHits()[0];

            for (int i = 0; i < needPageTurnNum; i++) {

                SearchRequest build = null;

                if (i != 0) {
                    documentFields = response.getHits().getHits()[NumberUtil.toInt(limit - 1)];
                }
                sourceAsMap = documentFields.getSourceAsMap();
                objs = new Object[]{NumberUtil.toLong(sourceAsMap.get("create_date")),documentFields.getId()};

                if (i == needPageTurnNum - 1) {
                    if (NumberUtil.toInt(end % limit) - pageSize - 1 >= 0) {
                        build = getBuilder(query).setAfterValues(objs)
                                .setFrom(0)
                                .setSize(NumberUtil.toInt(end % limit) - pageSize).build();
                        response = client.search(build, DEFAULT_REQUEST_OPTIONS);

                        SearchHit documentFieldsTemp = response.getHits().getHits()[NumberUtil.toInt(end % limit) - pageSize -1];
                        Map<String, Object> sourceAsMapTemp = documentFieldsTemp.getSourceAsMap();
                        objs = new Object[]{NumberUtil.toLong(sourceAsMapTemp.get("create_date")), documentFieldsTemp.getId()};
                        startIndex = 0;
                    }
                    build = getBuilder(query).setAfterValues(objs)
                            .setFrom(0)
                            .setSize(pageSize).build();
                } else {
                    build = getBuilder(query).setAfterValues(objs).setFrom(0).setSize(NumberUtil.toInt(limit)).build();
                }
                response = client.search(build,DEFAULT_REQUEST_OPTIONS);
            }
        }

        SearchHit[] hits = response.getHits().getHits();

        for (int i = startIndex, len = startIndex + pageSize; i < len; i++) {
            String id = hits[i].getId();
            long create_date = NumberUtil.toLong(hits[i].getSourceAsMap().get("create_date"));
            System.out.println(id + "\t" + create_date);
        }

    }

     public void searchWithScroll() {
    /*int pageIndex = Integer.parseInt(map.get("pageIndex"));
    int pageSize = Integer.parseInt(map.get("pageSize"));

    int start = (pageIndex - 1) * pageSize;
    int end = start + pageSize;

    ESRequestBuilder.Builder builder = ESRequestBuilder.builder()
            .setIndex("itemwithsku4")
            .setQuery(query)
            .addSort("create_date", SortOrder.DESC)
            .addSort("_id", SortOrder.DESC)
            .setFetchSource(true)
            .setFetchFields(new String[]{"create_date"});
    SearchRequest request = null;
    if (end > limit) {
        builder = builder.setScroll(new TimeValue(60000L))
                .setFrom(0)
                .setSize(NumberUtil.toInt(limit));
    } else {
        builder = builder.setFrom(start).setSize(pageSize);
    }
    request = builder.build();
    Long totalCount = esHelper.getTotalCount(query, "itemwithsku4");
    System.out.println(totalCount);
    SearchResponse response = client.search(request, DEFAULT_REQUEST_OPTIONS);
    if (end > limit) {
        String scrollId = response.getScrollId();
        int needPageTurnNum = NumberUtil.toInt(end / limit);
        for (int i = 0; i < needPageTurnNum; i++) {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
            searchScrollRequest.scrollId(scrollId).scroll(new TimeValue(60000L));
            response = client.scroll(searchScrollRequest, DEFAULT_REQUEST_OPTIONS);
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
clearScrollRequest.addScrollId(scrollId);
ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
boolean succeeded = clearScrollResponse.isSucceeded();


    }

    SearchHit[] hits = response.getHits().getHits();
    int startIndex = end > limit ? NumberUtil.toInt(start % limit) : 0;

    for (int i = startIndex, len = startIndex + pageSize; i < len; i++) {
        String id = hits[i].getId();
        long create_date = NumberUtil.toLong(hits[i].getSourceAsMap().get("create_date"));
        System.out.println(id + "\t" + create_date);
    }*/
}

}
