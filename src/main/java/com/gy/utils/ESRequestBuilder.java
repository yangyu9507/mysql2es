package com.gy.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.gy.model.FieldSort;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * created by yangyu on 2019-10-09
 */
@Component
public class ESRequestBuilder {

    private static RestHighLevelClient client;

    private static final RequestOptions DEFAULT_REQUEST_OPTIONS = RequestOptions.DEFAULT;

    private ESRequestBuilder(RestHighLevelClient restHighLevelClient){
        setClient(restHighLevelClient);
    }

    private synchronized static void setClient(RestHighLevelClient restHighLevelClient) {
        client = restHighLevelClient;
    }

    public static ESRequestBuilder.Builder builder(){
        return new Builder();
    }


    public static class Builder implements Cloneable{

        @Override
        public Builder clone() throws CloneNotSupportedException {
            Builder stu = null;
            stu = (Builder)super.clone();   //浅复制
            return stu;
        }

        //必须的参数
        private String[] indices;

        private QueryBuilder queryBuilder;
        private Integer from;
        private Integer size;
        private String[] fetchFields;
        private Boolean fetchSource;
        //字段排序集合
        private List<FieldSort> sorts;
        //聚合语句集合
        private Set<String> names = Sets.newConcurrentHashSet();
        private List<AggregationBuilder> aggregationBuilders = Collections.synchronizedList(Lists.newArrayList());
        private TimeValue keepAlive;
        private Long timeOut;
        private Object[] afterValues;

        private ThreadLocal<SearchRequest> requestBuilderThreadLocal = ThreadLocal.withInitial(SearchRequest::new);
        private ThreadLocal<SearchSourceBuilder> searchSourceBuilderThreadLocal = ThreadLocal.withInitial(SearchSourceBuilder::new);

        public Builder setIndex(String... indices) {
            Preconditions.checkArgument(null != indices && indices.length > 0, "query indices can not be empty");
            this.indices = indices;
            return this;
        }

        public Builder setQuery(QueryBuilder queryBuilder) {
            this.queryBuilder = queryBuilder;
            return this;
        }

        public Builder addAggregation(AggregationBuilder aggregationBuilder) {
            if (!names.add(aggregationBuilder.getName())) {
                throw new IllegalArgumentException("聚合名称重复: [" + aggregationBuilder.getName() + "]");
            }
            this.aggregationBuilders.add(aggregationBuilder);
            return this;
        }

        public Builder setFrom(Integer from) {
            this.from = from;
            return this;
        }

        public Builder setSize(Integer size) {
            this.size = size;
            return this;
        }

        public Builder setFetchSource(Boolean fetchSource) {
            this.fetchSource = fetchSource;
            return this;
        }

        public Builder setFetchFields(String[] fetchFields) {
            if (fetchFields != null) {
                this.fetchFields = fetchFields.clone();
            }
            return this;
        }

        public Builder addSort(String field, SortOrder order) {
            if (sorts == null) {
                sorts = new ArrayList<>();
            }
            sorts.add(new FieldSort(field, order));
            return this;
        }

        public Builder setScroll(TimeValue keepAlive) {
            if (keepAlive != null) {
                this.keepAlive = keepAlive;
            }
            return this;
        }

        public Builder setTimeOut(Long timeOut){
            if (Objects.nonNull(timeOut)){
                this.timeOut = timeOut;
            } else {
                this.timeOut = 10L;
            }
            return this;
        }

        public Builder setAfterValues(Object[] afterValues) {
            if (null != afterValues) {
                this.afterValues = afterValues;
            } else {
                this.afterValues = null;
            }
            return this;
        }


        public SearchRequest build() throws IOException{

            SearchRequest requestBuilder = requestBuilderThreadLocal.get();
            SearchSourceBuilder searchSourceBuilder = searchSourceBuilderThreadLocal.get();

            searchSourceBuilder.timeout(TimeValue.timeValueSeconds(60));

            requestBuilder.source(searchSourceBuilder);

            if (Objects.nonNull(this.indices) && this.indices.length > 0){
                requestBuilder.indices(this.indices);
            }
            if (Objects.nonNull(this.queryBuilder)) {
                searchSourceBuilder.query(this.queryBuilder);
            }
            if (CollectionUtils.isNotEmpty(this.aggregationBuilders)) {
                for (AggregationBuilder aggregationBuilder : aggregationBuilders) {
                    searchSourceBuilder.aggregation(aggregationBuilder);
                }
            }
            if (Objects.nonNull(fetchSource)) {
                searchSourceBuilder.fetchSource(fetchSource);
            }
            if (Objects.nonNull(fetchFields)) {
                searchSourceBuilder.fetchSource(fetchFields, null);
            }
            if (Objects.nonNull(from)) {
                searchSourceBuilder.from(from);
            }
            if (Objects.nonNull(size)) {
                searchSourceBuilder.size(size);
            }
            if (CollectionUtils.isNotEmpty(sorts)) {
                for (FieldSort sort : sorts) {
                    searchSourceBuilder.sort(sort.getField(), sort.getOrder());
                }
            }
            if (Objects.nonNull(keepAlive)) {
                requestBuilder.scroll(keepAlive);
            }

            if (Objects.nonNull(timeOut)) {
                searchSourceBuilder.timeout(TimeValue.timeValueSeconds(timeOut));
            }

            if (Objects.nonNull(afterValues) && afterValues.length > 0) {
                searchSourceBuilder.searchAfter(afterValues);
            }

            if (Objects.isNull(afterValues)){
                searchSourceBuilder.searchAfter();
            }


            requestBuilder.indicesOptions(IndicesOptions.fromOptions(
                    true, true, true,
                    false, IndicesOptions.strictExpandOpenAndForbidClosed()));

            return requestBuilder;
        }
    }

}
