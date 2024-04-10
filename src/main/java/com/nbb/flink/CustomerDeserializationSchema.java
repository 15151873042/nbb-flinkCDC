package com.nbb.flink;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * {
 *     "database": "gmall2021",
 *     "before": {
 *         "tm_name": "15",
 *         "logo_url": "15",
 *         "id": 15
 *     },
 *     "after": {
 *         "tm_name": "15",
 *         "logo_url": "16",
 *         "id": 15
 *     },
 *     "type": "update",
 *     "tableName": "base_trademark"
 * }
 */
public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {

    static final DateTimeFormatter LOCALDATE_FORMATTER =   DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter LOCALDATE_TIME_FORMATTER =   DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject rootJson = new JSONObject();

        // topic='mysql_binlog_source.gmall2021.base_trademark'
        // 获取databaeName和tableName
        String[] split = sourceRecord.topic().split("\\.");
        String databaseName = split[1];
        String tableName = split[2];

        Struct valueStruct = (Struct)sourceRecord.value();
        // 解析before
        JSONObject beforeJson = new JSONObject();

        Struct beforeStruct = valueStruct.getStruct("before");
        if (null != beforeStruct) {
            List<Field> beforeFields = beforeStruct.schema().fields();
            for (Field beforeField : beforeFields) {
                String key = beforeField.name();
                Object value  = beforeStruct.get(beforeField);

                // 类型转换
                value = fieldValueTransition(beforeField, value);

                beforeJson.put(key, value);
            }
        }
        // 解析after
        JSONObject afterJson = new JSONObject();
        Struct afterStruct = valueStruct.getStruct("after");
        if (null != afterStruct) {
            List<Field> afterFields = afterStruct.schema().fields();
            for (Field afterField : afterFields) {
                String key = afterField.name();
                Object value  = afterStruct.get(afterField);

                value = fieldValueTransition(afterField, value);
                afterJson.put(key, value);
            }
        }

        //获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation =
                Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        type = type.equals("create") ? "insert" : type;
        rootJson.put("database", databaseName);
        rootJson.put("tableName", tableName);
        rootJson.put("type", type);
        rootJson.put("before", beforeJson);
        rootJson.put("after", afterJson);

        collector.collect(rootJson.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }


    /**
     *
     * @param field
     * @param FieldValue
     * @return
     */
    Object fieldValueTransition(Field field, Object FieldValue) {

        String fieldSchemaTypeName = field.schema().type().getName();
        String fieldSchemaName = field.schema().name();


        if ("int32".equals(fieldSchemaTypeName) && "io.debezium.time.Date".equals(fieldSchemaName)) {
            // date类型默认转换成了距离1970-01-01日的天数，例如1970-01-02会转换成1，1969-12-31会转换成-1
            LocalDate localDate = LocalDate.of(1970, 1, 1).plusDays((int)FieldValue);
            return LOCALDATE_FORMATTER.format(localDate);
        }

        if ("int64".equals(fieldSchemaTypeName) && "io.debezium.time.Timestamp".equals(fieldSchemaName)) {
            // datetime类型会转换成时间戳，且用的是UTC时间（多+了8个小时）
            LocalDateTime localDateTime = LocalDateTime
                    .ofEpochSecond((long)FieldValue / 1000, 0, ZoneOffset.UTC);
            return LOCALDATE_TIME_FORMATTER.format(localDateTime);
        }

        return FieldValue;
    }


}
