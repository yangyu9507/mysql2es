package com.gy.controller;

import com.alibaba.fastjson.JSONObject;
import com.gy.enums.StatsEnum;
import com.gy.utils.ESRequestBuilder;
import com.gy.utils.esapi.AggApis;
import com.gy.utils.esapi.SearchApis;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * created by yangyu on 2019-10-16
 */
@RestController
@RequestMapping(value = "/agg")
@Slf4j
public class AggController {


    private static final String index = "orderdetail";

    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private SearchApis searchApis;
    @Autowired
    private AggApis aggApis;

    private static final String aggAlias = "agg";
    private static final String aggField = "amount";

    @GetMapping(value = "/get", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void test1(@RequestParam(value = "startTime")Long startTime,
                      @RequestParam(value = "endTime")Long endTime) throws Exception {

        BoolQueryBuilder query = boolQuery();
        query.filter(rangeQuery("createDate").from(startTime,false)
                                                    .to(endTime,false));

        String[] fields = {"shopTypeName", "shopName", "shopId"};

        TermsAggregationBuilder termsAgg =
                aggApis.multiFieldTermsAggBuilder("multiTerms",
                        fields,10,true,false);

        SearchRequest request = ESRequestBuilder.builder()
                .setIndex(index)
                .setQuery(query)
                .setFetchSource(false)
                .addAggregation(termsAgg)
                .build();
        long count = searchApis.count(query, index);
        log.info("count : {} ",count);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Terms terms = response.getAggregations().get("multiTerms");
        terms.getBuckets().stream().forEach(bucket -> System.out.println(bucket.getKeyAsString()+" -> " + bucket.getDocCount()));

    }

}
