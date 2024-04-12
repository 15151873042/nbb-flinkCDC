package com.nbb.flink;

import com.nbb.flink.sink.MyElasticsearchSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
                .tableList("cdc_test.*")
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.initial()) // 整库同步，全量 + 增量
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // FIXME 库同步，全量 + 增量同步时，必须开启checkpoint，否则增量数据无法获取
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 2.3、设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 指定CK自动重启策略（允许重启3次，如果重启失败后再次重启的间隔为2S）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        // 2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("file:///d:/flink/checkpoints"));

        DataStreamSource<String> mysqlDataSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "myqlSource");

        mysqlDataSource.sinkTo(MyElasticsearchSink.newSink(new HttpHost("127.0.0.1", 9200, "http")));
        mysqlDataSource.print();

        env.execute();
    }
}
