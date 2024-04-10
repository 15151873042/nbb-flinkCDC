package com.nbb.flink;

import com.nbb.flink.sink.MyElasticsearchSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;

public class MySql2Elasticsearch {

    /**
     * jdk17启动需要添加如下参数：
     * --add-opens java.base/java.util=ALL-UNNAMED
     */
    public static void main(String[] args) throws Exception {


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .serverTimeZone("Asia/Shanghai")
                .databaseList("cdc_test")
                .tableList("cdc_test.student")
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.initial()) // 整库同步，全量 + 增量
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // FIXME 库同步，全量 + 增量同步时，必须开启checkpoint，否则增量数据无法获取
        env.enableCheckpointing(5000L);

        DataStreamSource<String> mysqlDataSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "myqlSource");

        mysqlDataSource.sinkTo(MyElasticsearchSink.newSink(new HttpHost("127.0.0.1", 9200, "http")));
        mysqlDataSource.print();

        env.execute();
    }
}
