package com.nbb.flink.sink;

import cn.hutool.core.util.HexUtil;
import cn.hutool.core.util.ObjUtil;
import cn.hutool.crypto.SmUtil;
import cn.hutool.crypto.symmetric.SM4;
import com.nbb.flink.domain.CdcDO;
import io.debezium.data.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MyMySqlSink extends RichSinkFunction<CdcDO> {

    private String jdbcUrl;
    private String username;
    private String password;
    private Connection connection;

    private static final String sm4Key = "668ce654850b9c9d0d707e22740042d3";

    private static final SM4 sm4 = SmUtil.sm4(HexUtil.decodeHex(sm4Key));



    public MyMySqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(CdcDO value, Context context) throws Exception {
        Statement statement = null;
        try {
            List<String> sqlList = genSql(value);
            if (CollectionUtils.isEmpty(sqlList)) {
                return;
            }
            statement = connection.createStatement();
            for (String sql : sqlList) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } catch (BatchUpdateException e) {
            if (!e.getMessage().matches("Table.*doesn't exist")) {
                log.error(e.getMessage(), e);
                return;
            }

            String createTableSql = this.genCreateTableSql(value);
            statement.addBatch(createTableSql);

            List<String> sqlList = genSql(value);
            for (String sql : sqlList) {
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    public List<String> genSql(CdcDO value) {
        Envelope.Operation operation = value.getOperation();
        String tableName = value.getTableName();
        Map<String, Object> before = value.getBefore();
        Map<String, Object> after = value.getAfter();

        if (operation == Envelope.Operation.READ || operation == Envelope.Operation.CREATE) {
            Object id = after.get("id");
            Set<String> keys = after.keySet();
            Collection<Object> values = after.values();
            List<String> encryptValues = values.stream().map(v -> entryField(id, v)).collect(Collectors.toList());
            String insertSql = "insert into " + tableName + "(" +
                    StringUtils.join(keys, ",") + ") values('" +
                    StringUtils.join(encryptValues, "', '") + "')";
            return Collections.singletonList(insertSql);
        } else if (operation == Envelope.Operation.UPDATE){

            Object id = before.get("id");
            String deleteSql =  "delete from " + tableName + " where id = " + id;

            Set<String> keys = after.keySet();
            Collection<Object> values = after.values();
            List<String> encryptValues = values.stream().map(v -> entryField(id, v)).collect(Collectors.toList());
            String insertSql = "insert into " + tableName + "(" +
                    StringUtils.join(keys, ",") + ") values('" +
                    StringUtils.join(encryptValues, "', '") + "')";
            return Arrays.asList(deleteSql, insertSql);

        } else if (operation == Envelope.Operation.DELETE) {
            Object id = before.get("id");
            String deleteSql = "delete from " + tableName + " where id = " + id;
            return Collections.singletonList(deleteSql);
        }
        return Collections.emptyList();
    }

    private String genCreateTableSql(CdcDO sqlCdcDO) {

        Envelope.Operation operation = sqlCdcDO.getOperation();


        Map<String, Object> data;

        if (operation == Envelope.Operation.DELETE) {
            data = sqlCdcDO.getBefore();
        } else {
            data = sqlCdcDO.getAfter();
        }

        String tableName = sqlCdcDO.getTableName();

        Set<String> keys = data.keySet();

        String columnSql = data.keySet().stream()
                .filter(columnName -> !columnName.equals("id"))
                .map(columnName -> columnName + " longtext default null,")
                .collect(Collectors.joining(" "));

        return "create table IF NOT EXISTS " + tableName + "( id int not null, " + columnSql + " primary key (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    }

    private Map<String, Object> encrypt(Map<String, Object> originFieldMap) {
        return null;
    }

    public String entryField(Object id, Object fieldValue) {
        if (ObjUtil.isNull(fieldValue)) {
            return null;
        }
        if (id.equals(fieldValue)) {
            return id.toString();
        }

        SM4 sm4 = SmUtil.sm4(HexUtil.decodeHex(sm4Key));
        return sm4.encryptHex(fieldValue.toString());
    }

}
