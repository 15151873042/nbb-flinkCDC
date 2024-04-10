package com.nbb.flink.domain;

import io.debezium.data.Envelope;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Map;

/**
 * 一行mysql数据变更的封装类
 */
@Data
public class CdcDO implements Serializable {

    /** 数据库名称 */
    public String databaseName;

    /** 表名 */
    public String tableName;

    /** 新增、更新、删除 操作类型 */
    public Envelope.Operation operation;

    /** 变更前的数据信息 */
    public Map<String, Object> before;

    /** 变更后的数据信息 */
    public Map<String, Object> after;

    public CdcDO() {
    }
}
