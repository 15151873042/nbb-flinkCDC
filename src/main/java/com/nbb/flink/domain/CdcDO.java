package com.nbb.flink.domain;

import io.debezium.data.Envelope;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * mysql表中一行数据变更的封装类
 */
@Data
public class CdcDO implements Serializable {

    /** 数据库名称 */
    private String databaseName;

    /** 表名 */
    private String tableName;

    /** 新增、更新、删除 操作类型 */
    private Envelope.Operation operation;

    /** 变更前的数据行信息 */
    private Map<String, Object> before;

    /** 变更后的数据行信息 */
    private Map<String, Object> after;
}
