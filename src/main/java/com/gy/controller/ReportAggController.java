package com.gy.controller;

import com.google.common.collect.Lists;
import com.gy.enums.StatsEnum;
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
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * created by yangyu on 2019-10-22
 */
@RestController
@RequestMapping(value = "/report")
@Slf4j
public class ReportAggController {


    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private SearchApis searchApis;
    @Autowired
    private AggApis aggApis;

    private static final String index = "orderdetail";
    private static final int times = 20;

    // item_sname supplierOuterId packagePoint distributionDate
    private static final String[] getListCountTopHitFetchFields
            = {"categoryName","taxRate","typeName",
            "itemName","itemSkuName","costPrice",
            "platformItemName","platformSkuName","warehouseName"
            ,"shopName","originPrice", "itemBrandName","categoryId","skuId","itemUnit"};

    private static final String[] groupByFields = {"shopId","warehouseId","itemCode","itemSkuCode"};

    @GetMapping(value = "/getListCount", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public void getListCount(@RequestParam(value = "startTime")Long startTime,
                             @RequestParam(value = "endTime")Long endTime,
                             @RequestParam(value = "isDefault")Boolean isDefault) throws Exception {

        BoolQueryBuilder query = boolQuery();
        query.filter(rangeQuery("createDate").from(startTime,true)
                     .to(endTime,true));
        for (String existField: groupByFields) {
            query.filter(existsQuery(existField));
        }

        AggregationBuilder qtyAgg = aggApis.statsAggBuilder("qty", "qty", StatsEnum.Sum);
        AggregationBuilder amountAgg = aggApis.statsAggBuilder("amount", "amount", StatsEnum.Sum);
        AggregationBuilder amountAfterAgg = aggApis.statsAggBuilder("amountAfter", "amountAfter", StatsEnum.Sum);
        AggregationBuilder  postFeeAgg = aggApis.statsAggBuilder("postFee", "postFee", StatsEnum.Sum);
        AggregationBuilder originAmountAgg = aggApis.statsAggBuilder("originAmount", "originAmount", StatsEnum.Sum);

        TermsAggregationBuilder termsAgg = aggApis.multiFieldTermsAggBuilder("groupByAgg", groupByFields, 1000, true, false, isDefault);

        TopHitsAggregationBuilder topHitsAgg = aggApis.groupByAggBuilder("topHits", getListCountTopHitFetchFields, null, null, 0, 1000);
        termsAgg.subAggregation(topHitsAgg);

        termsAgg.subAggregation(qtyAgg);
        termsAgg.subAggregation(amountAgg);
        termsAgg.subAggregation(amountAfterAgg);
        termsAgg.subAggregation(postFeeAgg);
        termsAgg.subAggregation(originAmountAgg);
        SearchRequest request = ESRequestBuilder.builder()
                .setIndex(index)
                .setFetchSource(false)
                .setQuery(query)
                .addAggregation(termsAgg)
                .build();

        List<Long> tookList = Lists.newArrayList();
        for (int i = 0 ; i < times; i++){
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            tookList.add(response.getTook().getMillis());
        }
        tookList.stream().forEach(System.out::println);
        long sum = tookList.stream().mapToLong(item -> item).sum();
        System.out.println(NumberUtil.toDouble(sum / times));

        long count = searchApis.count(query, index);
        System.out.println(count);

    }

}
