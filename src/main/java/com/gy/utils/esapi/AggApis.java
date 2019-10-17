package com.gy.utils.esapi;

import com.google.common.base.Preconditions;
import com.gy.enums.StatsEnum;
import com.gy.model.FieldSort;
import gy.lib.common.util.NumberUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

/**
 * created by yangyu on 2019-10-16
 */
@Service
@Slf4j
public class AggApis {

    private static final Long Min_Doc_Count = 1L;
    private static final Long Shard_Min_Doc_Count = 0L;
    private static final Boolean Show_Term_Doc_Count_Error = false;
    private static final String Execution_Hint_Global_Ordinals = "global_ordinals";
    private static final String Execution_Hint_Map = "map";

    public TermsAggregationBuilder termsAggBuilder(String alias, String field,Integer size
                                                  ,Boolean orderKey,Boolean orderCount) {
        Preconditions.checkArgument(StringUtils.isNotBlank(alias),"alias must not be empty. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(field),"field must not be empty. ");


        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(alias).field(field);

        return termsBuilder(termsAgg,size,orderKey,orderCount);
    }

    private static final String splits = " + ' ***** ' + ";
    private static final String multiMultiFormat = "String.valueOf(doc['%s'].empty ? '': String.valueOf(doc['%s'].value))";

    public TermsAggregationBuilder multiFieldTermsAggBuilder(String alias, String[] field,Integer size
                                                            ,Boolean orderKey,Boolean orderCount) {

        Preconditions.checkArgument(StringUtils.isNotBlank(alias),"alias must not be empty. ");
        Preconditions.checkArgument(Objects.nonNull(field) && field.length > 1,"field.length must be lagger than 1, otherwise use single filed agg [termsAgg].");

        StringBuilder scriptStr = new StringBuilder();
        scriptStr.append(String.format(multiMultiFormat,field[0],field[0]));
        for (int i = 1,len = field.length; i < len ; i++) {
            scriptStr.append(splits);
            scriptStr.append(String.format(multiMultiFormat,field[i],field[i]));
        }

        Script script = new Script(scriptStr.toString());
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(alias).script(script);

        return termsBuilder(termsAgg, size, orderKey, orderCount);
    }


    private TermsAggregationBuilder termsBuilder(TermsAggregationBuilder termsAgg,Integer size,Boolean orderKey,Boolean orderCount){

        if (Objects.isNull(size)){
            size = 10;
        }

        termsAgg.size(size);

        Integer shardSize = NumberUtil.toInt(size * 1.5) + 10;

        // 为了提高结果的精确性
        termsAgg.shardSize(shardSize);

        // 默认深度优先聚合,改为 广度优先
        termsAgg.collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
        // 默认方式
        termsAgg.executionHint(Execution_Hint_Global_Ordinals);

        // 默认 "_count": "desc"  以terms数量倒序
        termsAgg.order(BucketOrder.count(Objects.nonNull(orderCount) ? orderCount: false));
        // 默认  "_key": "asc"    以terms的key的字典顺序 正序
        termsAgg.order(BucketOrder.key(Objects.nonNull(orderKey)? orderKey : true));

        termsAgg.minDocCount(Min_Doc_Count);
        termsAgg.shardMinDocCount(Shard_Min_Doc_Count);
        // show_term_doc_count_error可以查看每个聚合误算的最大值
        termsAgg.showTermDocCountError(Show_Term_Doc_Count_Error);

        return termsAgg;
    }

    /**
     * <p>数字类型字段百分比占用统计</p>
     * @param alias
     * @param field
     * @return
     */
    public PercentilesAggregationBuilder percentilesAggBuilder(String alias,String field){
        Preconditions.checkArgument(StringUtils.isNotBlank(alias),"alias must not be empty. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(field),"field must not be empty. ");
        return AggregationBuilders.percentiles(alias).field(field);
    }


    /**
     * <p>获取聚合对象</p>
     *
     * @param alias      聚合别名
     * @param field      聚合字段
     * @param statsEnum  聚合种类 {@link StatsEnum}
     * @return 聚合对象
     */
    public AggregationBuilder statsAggBuilder(String alias, String field, StatsEnum statsEnum){
        Preconditions.checkArgument(StringUtils.isNotBlank(alias),"AvgAlias must not be empty. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(field),"Avg field must not be empty. ");
        switch (statsEnum) {
            case Avg:
                return AggregationBuilders.avg(alias).field(field);
            case Count:
                return AggregationBuilders.count(alias).field(field);
            case Max:
                return AggregationBuilders.max(alias).field(field);
            case Min:
                return AggregationBuilders.min(alias).field(field);
            case Sum:
                return AggregationBuilders.sum(alias).field(field);
            default:
                return AggregationBuilders.extendedStats(alias).field(field);
        }
    }

    /**
     * <p>获取相应统计聚合的结果</p>
     *
     * @param response   ES响应
     * @param alias      聚合别名
     * @param statsEnum  聚合种类
     * @return 聚合结果
     */
    public String getStatsAggValue(SearchResponse response,String alias, StatsEnum statsEnum) {
        Preconditions.checkArgument(Objects.nonNull(response),"response must not be empty. ");
        Aggregations aggregations = response.getAggregations();
        Preconditions.checkArgument(Objects.nonNull(aggregations),"aggregations must not be empty. ");
        Aggregation aggregation = aggregations.get(alias);

        switch (statsEnum) {
            case Avg:
                return ((Avg)aggregation).getValueAsString();
            case Count:
                return String.valueOf(((ValueCount)aggregation).getValue());
            case Max:
                return ((Max)aggregation).getValueAsString();
            case Min:
                return ((Min)aggregation).getValueAsString();
            case Sum:
                return ((Sum)aggregation).getValueAsString();
            case StdDeviation:
                return ((ExtendedStats)aggregation).getStdDeviationAsString();
            case SumOfSquares:
                return ((ExtendedStats)aggregation).getSumOfSquaresAsString();
            case Variance:
                return ((ExtendedStats)aggregation).getVarianceAsString();
            default:
               return "";
        }
    }

    /**
     * <p>对字段进行Distinct数量查询,类似数据库的distinct count</p>
     *
     * @param distinctCountAlias 聚合别名
     * @param field              查询字段
     * @return
     */
    public CardinalityAggregationBuilder distinctCountAggBuilder(String distinctCountAlias, String field){
        Preconditions.checkArgument(StringUtils.isNotBlank(distinctCountAlias),"distinctCountAlias must not be empty. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(field),"DistinctCount Avg field must not be empty. ");
        return AggregationBuilders.cardinality(distinctCountAlias).field(field);
    }

    /**
     * <p>返回 对字段进行Distinct数量查询的结果 </p>
     *
     * @param response            ES响应
     * @param distinctCountAlias  聚合别名
     * @return
     */
    public long getdistinctCountAggValue(SearchResponse response,String distinctCountAlias) {
        Preconditions.checkArgument(Objects.nonNull(response),"response must not be empty. ");
        Aggregations aggregations = response.getAggregations();
        Preconditions.checkArgument(Objects.nonNull(aggregations),"aggregations must not be empty. ");
        Cardinality cardinality = aggregations.get(distinctCountAlias);
        if (Objects.isNull(cardinality)){
            throw new NullPointerException("cardinalityAggregation is null");
        }

        return cardinality.getValue();
    }

    /**
     * <p>统计聚合|类似group by</p>
     * @param aggAlias          聚合别名
     * @param groupByField      groupBy字段
     * @param fetchFields       需要取的字段
     * @param excludeFields     需要排除的字段
     * @param sortList          排序字段
     * @param from              起始取值位置
     * @param size              取值数量
     * @Return                  统计聚合对象
     */
    public TopHitsAggregationBuilder groupByAggBuilder(String aggAlias, String groupByField, String[] fetchFields,String[] excludeFields,
                                  List<FieldSort> sortList,Integer from,Integer size) {
        Preconditions.checkArgument(Objects.nonNull(aggAlias),"aggAlias must not be empty. ");
        Preconditions.checkArgument(StringUtils.isNotBlank(groupByField),"groupByField must not be empty. ");

        TopHitsAggregationBuilder topHitsAgg = AggregationBuilders.topHits(aggAlias)
                .docValueField(groupByField)
                .fetchSource(Objects.nonNull(fetchFields))
                .fetchSource(fetchFields, excludeFields);

        if (Objects.nonNull(from) && Objects.nonNull(size)) {
            Preconditions.checkArgument(from >= 0,"from must bigger than 0 ");
            Preconditions.checkArgument(size >= 0,"size must bigger than 0");
        }

        if (Objects.isNull(from)) {
            from = 0;
        }
        if (Objects.isNull(size)){
            size = 10;
        }

        topHitsAgg.from(from).size(size);

        if (CollectionUtils.isNotEmpty(sortList)) {
            for (FieldSort sortField : sortList) {
                topHitsAgg.sort(sortField.getField(), sortField.getOrder());
            }
        }

        return topHitsAgg;
    }





}
