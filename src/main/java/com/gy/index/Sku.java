package com.gy.index;

import org.zxp.esclientrhl.annotation.ESID;
import org.zxp.esclientrhl.annotation.ESMapping;
import org.zxp.esclientrhl.annotation.ESMetaData;
import org.zxp.esclientrhl.enums.DataType;

/**
 * created by yangyu on 2019-09-17
 */
@ESMetaData(indexName = "mysqltoessku", indexType = "mysqltoessku",number_of_shards = 5,number_of_replicas = 0, printLog = true, refresh_interval = "5s")
public class Sku {

    @ESID
    private Long id;

    @ESMapping(datatype = DataType.long_type)
    private Long create_date;

    @ESMapping(datatype = DataType.long_type)
    private String modify_date;

    @ESMapping(datatype = DataType.integer_type)
    private Integer version;

    @ESMapping(datatype = DataType.long_type)
    private Long tenant_id;

    @ESMapping(datatype = DataType.long_type)
    private Long item_id;

    @ESMapping(datatype = DataType.integer_type)
    private Long del;

    @ESMapping(datatype = DataType.long_type)
    private Long item_sku_id;

    @ESMapping(datatype = DataType.integer_type)
    private Long combine;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCreate_date() {
        return create_date;
    }

    public void setCreate_date(Long create_date) {
        this.create_date = create_date;
    }

    public String getModify_date() {
        return modify_date;
    }

    public void setModify_date(String modify_date) {
        this.modify_date = modify_date;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Long getTenant_id() {
        return tenant_id;
    }

    public void setTenant_id(Long tenant_id) {
        this.tenant_id = tenant_id;
    }

    public Long getItem_id() {
        return item_id;
    }

    public void setItem_id(Long item_id) {
        this.item_id = item_id;
    }

    public Long getDel() {
        return del;
    }

    public void setDel(Long del) {
        this.del = del;
    }

    public Long getItem_sku_id() {
        return item_sku_id;
    }

    public void setItem_sku_id(Long item_sku_id) {
        this.item_sku_id = item_sku_id;
    }

    public Long getCombine() {
        return combine;
    }

    public void setCombine(Long combine) {
        this.combine = combine;
    }
}
