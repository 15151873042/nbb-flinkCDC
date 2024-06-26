package com.nbb.flink;

import com.nbb.flink.domain.CdcDO;
import com.nbb.flink.schema.MyDeserializationSchema;
import com.nbb.flink.sink.MyMySqlSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySql2Mysql {

    /**
     * jdk17启动需要添加如下参数：
     * --add-opens java.base/java.util=ALL-UNNAMED
     */
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        // FIXME 库同步，全量 + 增量同步时，必须开启checkpoint，否则增量数据无法获取
        env.enableCheckpointing(10000L);

        MySqlSource<CdcDO> mySqlSource = MySqlSource.<CdcDO>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .serverTimeZone("Asia/Shanghai")
                .databaseList("flink_cdc")
                .tableList("flink_cdc.*")
                .deserializer(new MyDeserializationSchema())
                .startupOptions(StartupOptions.initial()) // 整库同步，全量 + 增量
                .build();

        DataStreamSource<CdcDO> mysqlDataSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "myqlSource");

        mysqlDataSource.addSink(new MyMySqlSink("jdbc:mysql://127.0.0.1:3306/flink_cdc_sink", "root", "root"));
        mysqlDataSource.print();

        env.execute();
    }

}
