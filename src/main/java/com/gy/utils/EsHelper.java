package com.gy.utils;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * created by yangyu on 2019-10-09
 */
@Service
public class EsHelper {

    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;

    @Autowired
    private RestHighLevelClient client;

    public Long getTotalCount(BoolQueryBuilder query, String... indices) throws IOException {
        CountRequest countRequest = new CountRequest();
        countRequest.indices(indices);
        countRequest.source(new SearchSourceBuilder().query(query));
        CountResponse response = client.count(countRequest, DEFAULT_REQUEST_OPTIONS);
        return response.getCount();
    }


}
