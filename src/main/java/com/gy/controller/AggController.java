package com.gy.controller;

import com.google.common.collect.Lists;
import com.gy.enums.StatsEnum;
import com.gy.model.FieldSort;
import com.gy.utils.ESRequestBuilder;
import com.gy.utils.esapi.AggApis;
import com.gy.utils.esapi.SearchApis;
import gy.lib.common.util.NumberUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    private static final String[] topHitFields = {"qty","amount","discountFee","amountAfter"};

    private static final String[] groupByFields
            = {"taxRate","categoryId","skuId","categoryName","itemCode",
            "itemName","itemSkuCode","simpleName",
            "itemSkuName","costPrice","itemBrandName","itemUnit"};

    @GetMapping(value = "/get", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void test1(@RequestParam(value = "startTime")Long startTime,
                      @RequestParam(value = "endTime")Long endTime) throws Exception {

        BoolQueryBuilder query = boolQuery();
        query.filter(rangeQuery("createDate").from(startTime,true).to(endTime,true));
        for (int i = 0 , len = groupByFields.length ; i < len ; i++) {
            query.filter(existsQuery(groupByFields[i]));
        }

        /*TermsAggregationBuilder termsAgg =
                aggApis.multiFieldTermsAggBuilder("multiTerms",
                        groupByFields,10,null,null);*/

        TermsAggregationBuilder termsAgg =
                aggApis.multiFieldTermsAggBuilder("agg", groupByFields, 10, null, null,true);

        List<FieldSort> sorts = Lists.newArrayList();
        sorts.add(new FieldSort("categoryName",SortOrder.ASC));
        sorts.add(new FieldSort("itemCode",SortOrder.ASC));
        sorts.add(new FieldSort("itemSkuCode",SortOrder.ASC));

        TopHitsAggregationBuilder topAgg
                = aggApis.groupByAggBuilder("topAgg",  topHitFields, null, sorts, 0, 10);

        termsAgg.subAggregation(topAgg);

        AggregationBuilder qtySum = aggApis.statsAggBuilder("qty_sum", "qty", StatsEnum.Sum);
        AggregationBuilder amountSum = aggApis.statsAggBuilder("amount_sum", "amount", StatsEnum.Sum);
        AggregationBuilder discountFeeSum = aggApis.statsAggBuilder("discountFee_sum", "discountFee", StatsEnum.Sum);
        AggregationBuilder amountAfterSum = aggApis.statsAggBuilder("amountAfter_sum", "amountAfter", StatsEnum.Sum);
        termsAgg.subAggregation(qtySum);
        termsAgg.subAggregation(amountSum);
        termsAgg.subAggregation(discountFeeSum);
        termsAgg.subAggregation(amountAfterSum);

        BucketOrder amountSumAggregation = BucketOrder.aggregation("amount_sum", false);
        termsAgg.order(amountSumAggregation);

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


    @GetMapping(value = "/execution_hint", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void execution_hint(@RequestParam(value = "startTime")Long startTime,
                               @RequestParam(value = "endTime")Long endTime,
                               @RequestParam(value = "isDefault")Boolean isDefault) throws Exception {

        BoolQueryBuilder query = boolQuery();
//        query.filter(wildcardQuery("code","*835*"));

        query.filter(rangeQuery("createDate").from(startTime,true).to(endTime,true));

        TermsAggregationBuilder termsAgg = aggApis.termsAggBuilder("shopType", "shopType", 1000, false, false, isDefault)
                .subAggregation(aggApis.termsAggBuilder("shopName", "shopName", 1000, false, false, isDefault)
                        .subAggregation(aggApis.termsAggBuilder("itemType", "itemType", 1000, false, false, isDefault)
                                .subAggregation(aggApis.termsAggBuilder("itemName", "itemName", 1000, false, false, isDefault)))
                        );

        SearchRequest request = ESRequestBuilder.builder()
                .setIndex(index)
                .setQuery(query)
                .addAggregation(termsAgg)
                .build();

        List<Long> tookList = Lists.newArrayList();

        for (int i = 0;i<20;i++){
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            tookList.add(response.getTook().getMillis());
        }
        tookList.stream().forEach(System.out::println);
        long sum = tookList.stream().mapToLong(item -> item).sum();
        System.out.println(NumberUtil.toDouble(sum / 20));
        long count = searchApis.count(query, index);

        System.out.println(count);
        /*SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        Terms terms = response.getAggregations().get("shopType");


        for (Terms.Bucket shopTypeBucket : terms.getBuckets()){
            String shopType = shopTypeBucket.getKeyAsString();
            Terms shopNameTerms = shopTypeBucket.getAggregations().get("shopName");
            for (Terms.Bucket shopNameBucket : shopNameTerms.getBuckets()) {
                String shopName = shopNameBucket.getKeyAsString();
                Terms itemTypeTerms = shopNameBucket.getAggregations().get("itemType");
                for (Terms.Bucket itemTypeBucket : itemTypeTerms.getBuckets()) {
                    String itemType = itemTypeBucket.getKeyAsString();
                    Terms itemNameTerms = itemTypeBucket.getAggregations().get("itemName");
                    for (Terms.Bucket itemNameBucket : itemNameTerms.getBuckets()) {
                        String itemName = itemNameBucket.getKeyAsString();
                        System.out.println(shopType+"\t"+shopName+"\t"+itemType+"\t"+itemName);
                    }
                }
            }
        }*/
    }

}
