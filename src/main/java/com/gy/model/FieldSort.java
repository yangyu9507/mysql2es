package com.gy.model;

import org.elasticsearch.search.sort.SortOrder;

/**
 * <p>用于ES请求中排序的对象</p>
 *
 * created by yangyu on 2019-10-09
 */
public class FieldSort {

    private String field;
    private SortOrder order;

    public FieldSort(String field, SortOrder order) {
        this.field = field;
        this.order = order;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

}
