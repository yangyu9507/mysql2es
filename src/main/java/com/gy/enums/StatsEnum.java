package com.gy.enums;

/**
 * 统计方式枚举值
 *
 * created by yangyu on 2019-10-16
 */
public enum StatsEnum {

    Avg("avg","平均数"),

    Count("count","总数"),

    Max("max","最大值"),

    Min("min","最小值"),

    StdDeviation("stdDeviation","标准差"),

    Sum("sum","总和"),

    SumOfSquares("sumOfSquares","平方和"),

    Variance("variance","方差");

    private String code;
    private String name;

    StatsEnum(String code,String name){
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

}
