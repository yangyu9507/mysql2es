package com.gy.utils.esapi;

import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * created by yangyu on 2019-10-10
 */
@Service
public class SearchApis {

    private static final Logger logger = LoggerFactory.getLogger(SearchApis.class);
    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;

    @Autowired
    private RestHighLevelClient client;


    public boolean clearScroll(String scrollId) throws IOException {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);

        ClearScrollResponse clearScrollResponse = client.clearScroll(request, RequestOptions.DEFAULT);
        return clearScrollResponse.isSucceeded();
    }

    public MultiSearchResponse.Item[] multiSearch(List<SearchRequest> requestsList) throws IOException {
        MultiSearchRequest request = new MultiSearchRequest();
        for (SearchRequest req : requestsList){
            request.add(req);
        }
        MultiSearchResponse response = client.msearch(request, DEFAULT_REQUEST_OPTIONS);

        return response.getResponses();
    }

    public long count(QueryBuilder query,String...indices) throws IOException {
        CountRequest countRequest = new CountRequest();
        countRequest.indices(indices);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        countRequest.source(searchSourceBuilder);

        countRequest.indicesOptions(IndicesOptions.fromOptions(
                true, true, true,
                false, IndicesOptions.strictExpandOpenAndForbidClosed()));
        CountResponse countResponse = client
                .count(countRequest, RequestOptions.DEFAULT);

        return countResponse.getCount();
    }


}
